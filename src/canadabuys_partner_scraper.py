#!/usr/bin/env python3
"""
CanadaBuys partnering company scraper
------------------------------------

Scrapes the "Businesses interested in partnering" list from a CanadaBuys tender notice,
opens each company detail page, extracts company background/details, and exports the
results to both Excel and CSV.

Usage:
    python canadabuys_partner_scraper.py
    python canadabuys_partner_scraper.py --url "https://canadabuys.canada.ca/en/tender-opportunities/tender-notice/ws4286933967-doc4822970058"
    python canadabuys_partner_scraper.py --outdir output --concurrency 12

Optional LLM enrichment:
    Set OPENAI_API_KEY and pass --llm-summary to generate a concise normalized summary
    from the scraped long-form description using the OpenAI Responses API.

Install:
    pip install httpx[http2] beautifulsoup4 lxml pandas openpyxl tenacity tqdm
    # Optional for LLM summaries:
    pip install openai
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import httpx
import pandas as pd
from bs4 import BeautifulSoup, Tag
from tenacity import retry, stop_after_attempt, wait_exponential


DEFAULT_URL = "https://canadabuys.canada.ca/en/tender-opportunities/tender-notice/ws4286933967-doc4822970058"
BASE_URL = "https://canadabuys.canada.ca"


@dataclass
class CompanyRecord:
    tender_url: str
    tender_title: str | None = None
    tender_solicitation_number: str | None = None
    tender_publication_date: str | None = None
    tender_closing_datetime: str | None = None

    company_name: str | None = None
    partner_page_url: str | None = None
    company_website: str | None = None
    company_tagline: str | None = None
    company_description: str | None = None

    contact_first_name: str | None = None
    contact_last_name: str | None = None
    contact_full_name: str | None = None
    contact_title: str | None = None
    contact_phone: str | None = None
    contact_email: str | None = None

    other_links_json: str | None = None
    date_modified: str | None = None

    scraped_ok: bool = False
    scrape_error: str | None = None

    # Optional LLM normalized fields
    llm_one_line_summary: str | None = None
    llm_industry: str | None = None
    llm_capabilities_json: str | None = None


def clean_text(text: str | None) -> str | None:
    if text is None:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def safe_json_dumps(obj) -> str | None:
    if obj in (None, {}, [], ""):
        return None
    return json.dumps(obj, ensure_ascii=False)


def text_after_label(container: BeautifulSoup | Tag, label: str) -> str | None:
    """
    Pulls text that appears immediately after a label string anywhere in the page text.
    Useful because CanadaBuys pages are fairly regular but not strongly semantic.
    """
    text = container.get_text("\n", strip=True)
    pattern = rf"{re.escape(label)}\s*(.+?)(?:\n[A-Z][^\n]*\n|\n### |\n## |\Z)"
    m = re.search(pattern, text, flags=re.DOTALL)
    if not m:
        return None
    return clean_text(m.group(1))


def extract_email(text: str) -> str | None:
    m = re.search(r'[\w.+-]+@[\w.-]+\.\w+', text)
    return m.group(0) if m else None


def extract_phone(text: str) -> str | None:
    # Captures North American formats commonly used on these pages.
    patterns = [
        r'Telephone\s*[:\-]?\s*(\(?\d{3}\)?[\s\-]\d{3}[\s\-]\d{4})',
        r'(\(?\d{3}\)?[\s\-]\d{3}[\s\-]\d{4})',
    ]
    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            return clean_text(m.group(1))
    return None


def get_first_anchor_href_near_text(soup: BeautifulSoup, text_snippet: str) -> str | None:
    snippet = soup.find(string=lambda s: isinstance(s, str) and text_snippet.lower() in s.lower())
    if not snippet:
        return None
    parent = snippet.parent
    if not parent:
        return None
    a = parent.find("a", href=True)
    if a:
        return urljoin(BASE_URL, a["href"])
    # fallback: search near siblings
    nxt = parent.find_next("a", href=True)
    if nxt:
        return urljoin(BASE_URL, nxt["href"])
    return None


def parse_tender_metadata(soup: BeautifulSoup, tender_url: str) -> dict:
    text = soup.get_text("\n", strip=True)
    title = None
    h1 = soup.find("h1")
    if h1:
        title = clean_text(h1.get_text(" ", strip=True))

    solicitation_number = None
    m = re.search(r"Solicitation number\s+([A-Z0-9]+)", text, flags=re.IGNORECASE)
    if m:
        solicitation_number = m.group(1).strip()

    publication_date = None
    m = re.search(r"Publication date\s+([0-9]{4}/[0-9]{2}/[0-9]{2})", text)
    if m:
        publication_date = m.group(1)

    closing_dt = None
    m = re.search(r"Closing date and time\s+([0-9]{4}/[0-9]{2}/[0-9]{2}\s+[0-9]{2}:[0-9]{2}\s+[A-Z]{2,4})", text)
    if m:
        closing_dt = m.group(1)

    return {
        "tender_url": tender_url,
        "tender_title": title,
        "tender_solicitation_number": solicitation_number,
        "tender_publication_date": publication_date,
        "tender_closing_datetime": closing_dt,
    }


def parse_partner_links(soup: BeautifulSoup) -> list[tuple[str, str]]:
    """
    Grabs company detail links from the 'interested in partnering' section.
    We intentionally filter to CanadaBuys preview pages, which are the company detail pages.
    """
    partners: list[tuple[str, str]] = []

    # Strategy 1: all preview links on page
    for a in soup.select('a[href]'):
        href = a.get("href", "")
        name = clean_text(a.get_text(" ", strip=True))
        if not name:
            continue
        full_url = urljoin(BASE_URL, href)
        if "/node/preview/" in full_url:
            partners.append((name, full_url))

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for name, url in partners:
        key = (name.lower(), url)
        if key not in seen:
            seen.add(key)
            unique.append((name, url))
    return unique


def parse_company_page(html: str, page_url: str, tender_meta: dict) -> CompanyRecord:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)

    name = None
    h1 = soup.find("h1")
    if h1:
        name = clean_text(h1.get_text(" ", strip=True))

    website = None
    website_label_node = soup.find(string=lambda s: isinstance(s, str) and "Company website" in s)
    if website_label_node:
        parent = website_label_node.parent if isinstance(website_label_node, str) is False else None
    # More reliable fallback: first anchor after the label
    website = get_first_anchor_href_near_text(soup, "Company website")

    # Description block usually sits between company website and "First name"
    desc_text = None
    tagline = None
    page_text = soup.get_text("\n", strip=True)
    desc_match = re.search(
        r"Company website.*?\n(.+?)\nFirst name\s+",
        page_text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if desc_match:
        desc_block = clean_text(desc_match.group(1))
        if desc_block:
            parts = [p.strip() for p in re.split(r"\n+", desc_match.group(1).strip()) if p.strip()]
            if parts:
                tagline = clean_text(parts[0])
                desc_text = clean_text(" ".join(parts[1:]) if len(parts) > 1 else parts[0])

    # Fallback if regex above fails
    if not desc_text:
        lines = [clean_text(x) for x in page_text.splitlines()]
        lines = [x for x in lines if x]
        try:
            start_idx = next(i for i, x in enumerate(lines) if x.startswith("Company website"))
            end_idx = next(i for i, x in enumerate(lines) if x == "First name")
            block = lines[start_idx + 1:end_idx]
            if block:
                if block[0].startswith("http"):
                    block = block[1:]
                if block:
                    tagline = block[0]
                    desc_text = clean_text(" ".join(block[1:]) if len(block) > 1 else block[0])
        except StopIteration:
            pass

    first_name = None
    last_name = None
    contact_title = None
    date_modified = None

    m = re.search(r"First name\s+([^\n]+)", page_text, flags=re.IGNORECASE)
    if m:
        first_name = clean_text(m.group(1))
    m = re.search(r"Last name\s+([^\n]+)", page_text, flags=re.IGNORECASE)
    if m:
        last_name = clean_text(m.group(1))
    m = re.search(r"Title/position\s+([^\n]+)", page_text, flags=re.IGNORECASE)
    if m:
        contact_title = clean_text(m.group(1))
    m = re.search(r"Date modified:\s+([0-9]{4}-[0-9]{2}-[0-9]{2})", page_text, flags=re.IGNORECASE)
    if m:
        date_modified = m.group(1)

    email = extract_email(page_text)
    phone = extract_phone(page_text)
    full_name = clean_text(" ".join(x for x in [first_name, last_name] if x))

    other_links = []
    other_links_header = soup.find(string=lambda s: isinstance(s, str) and "Other links" in s)
    if other_links_header:
        # Gather anchors after header until page utility/footer links begin
        for a in soup.select("a[href]"):
            href = urljoin(BASE_URL, a["href"])
            label = clean_text(a.get_text(" ", strip=True))
            if not href or not label:
                continue
            if href == page_url:
                continue
            if "Report a problem on this page" in label:
                continue
            if label.startswith("LinkedIn") or ("linkedin.com" in href) or (website and href != website and "canadabuys.canada.ca" not in href):
                other_links.append({"label": label, "url": href})

    # Deduplicate links
    seen_links = set()
    deduped_links = []
    for item in other_links:
        key = (item["label"], item["url"])
        if key not in seen_links:
            seen_links.add(key)
            deduped_links.append(item)

    return CompanyRecord(
        **tender_meta,
        company_name=name,
        partner_page_url=page_url,
        company_website=website,
        company_tagline=tagline,
        company_description=desc_text,
        contact_first_name=first_name,
        contact_last_name=last_name,
        contact_full_name=full_name,
        contact_title=contact_title,
        contact_phone=phone,
        contact_email=email,
        other_links_json=safe_json_dumps(deduped_links),
        date_modified=date_modified,
        scraped_ok=True,
    )


@retry(wait=wait_exponential(multiplier=1, min=1, max=12), stop=stop_after_attempt(4))
async def fetch_text(client: httpx.AsyncClient, url: str) -> str:
    resp = await client.get(url, follow_redirects=True, timeout=45.0)
    resp.raise_for_status()
    return resp.text


async def scrape_company(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    company_name: str,
    page_url: str,
    tender_meta: dict,
    enable_llm: bool = False,
) -> CompanyRecord:
    async with semaphore:
        try:
            html = await fetch_text(client, page_url)
            record = parse_company_page(html, page_url, tender_meta)
            if enable_llm:
                await enrich_with_llm(record)
            return record
        except Exception as e:
            return CompanyRecord(
                **tender_meta,
                company_name=company_name,
                partner_page_url=page_url,
                scraped_ok=False,
                scrape_error=f"{type(e).__name__}: {e}",
            )


async def enrich_with_llm(record: CompanyRecord) -> None:
    """
    Optional cleanup step:
    Uses OpenAI to normalize free-form descriptions into a short summary / industry / capabilities.
    """
    if not record.company_description:
        return
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return

    try:
        from openai import AsyncOpenAI
    except Exception:
        return

    client = AsyncOpenAI(api_key=api_key)
    prompt = f"""
Normalize the following company profile from a government procurement partnering page.

Company: {record.company_name}
Website: {record.company_website}
Tagline: {record.company_tagline}
Description: {record.company_description}

Return strict JSON with keys:
- one_line_summary
- industry
- capabilities (array of 3 to 8 concise items)

Rules:
- Preserve factual meaning
- Do not invent facts
- Keep one_line_summary under 30 words
- Keep industry under 8 words
"""

    response = await client.responses.create(
        model="gpt-5-mini",
        input=prompt,
        temperature=0,
    )
    text = response.output_text.strip()
    try:
        payload = json.loads(text)
    except Exception:
        return

    record.llm_one_line_summary = clean_text(payload.get("one_line_summary"))
    record.llm_industry = clean_text(payload.get("industry"))
    record.llm_capabilities_json = safe_json_dumps(payload.get("capabilities"))


async def scrape_all(tender_url: str, outdir: Path, concurrency: int, enable_llm: bool) -> pd.DataFrame:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; CanadaBuysScraper/1.0; +https://example.local)"
    }

    async with httpx.AsyncClient(headers=headers, http2=True) as client:
        tender_html = await fetch_text(client, tender_url)
        tender_soup = BeautifulSoup(tender_html, "lxml")

        tender_meta = parse_tender_metadata(tender_soup, tender_url)
        partner_links = parse_partner_links(tender_soup)

        if not partner_links:
            raise RuntimeError("No partner company links were found on the tender page.")

        print(f"Found {len(partner_links)} partner links")

        semaphore = asyncio.Semaphore(concurrency)
        tasks = [
            scrape_company(
                client=client,
                semaphore=semaphore,
                company_name=name,
                page_url=url,
                tender_meta=tender_meta,
                enable_llm=enable_llm,
            )
            for name, url in partner_links
        ]

        results = await asyncio.gather(*tasks)
        df = pd.DataFrame([asdict(r) for r in results])

        # Normalize duplicates: the same company sometimes appears more than once
        df["company_name_normalized"] = (
            df["company_name"]
            .fillna("")
            .str.lower()
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

        outdir.mkdir(parents=True, exist_ok=True)
        csv_path = outdir / "canadabuys_ai_source_list_partners.csv"
        xlsx_path = outdir / "canadabuys_ai_source_list_partners.xlsx"
        json_path = outdir / "canadabuys_ai_source_list_partners.json"

        # Main flat export
        df.to_csv(csv_path, index=False)

        # Multi-sheet Excel export
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="companies", index=False)

            deduped = (
                df.sort_values(["company_name", "partner_page_url"])
                  .drop_duplicates(subset=["company_name_normalized"], keep="first")
            )
            deduped.to_excel(writer, sheet_name="deduped_companies", index=False)

            failures = df.loc[~df["scraped_ok"].fillna(False)].copy()
            failures.to_excel(writer, sheet_name="failures", index=False)

        df.to_json(json_path, orient="records", force_ascii=False, indent=2)

        print(f"Saved CSV  -> {csv_path}")
        print(f"Saved XLSX -> {xlsx_path}")
        print(f"Saved JSON -> {json_path}")

        return df


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Scrape CanadaBuys partnering companies into DataFrame/Excel.")
    p.add_argument("--url", default=DEFAULT_URL, help="CanadaBuys tender notice URL")
    p.add_argument("--outdir", default="output", help="Output directory")
    p.add_argument("--concurrency", type=int, default=12, help="Concurrent fetches for company pages")
    p.add_argument("--llm-summary", action="store_true", help="Use OpenAI to normalize descriptions into concise fields")
    return p


def main() -> None:
    args = build_parser().parse_args()
    outdir = Path(args.outdir)
    df = asyncio.run(
        scrape_all(
            tender_url=args.url,
            outdir=outdir,
            concurrency=args.concurrency,
            enable_llm=args.llm_summary,
        )
    )
    print()
    print(df.head(10).to_string(index=False))
    print()
    print(f"Rows scraped: {len(df)} | Successes: {int(df['scraped_ok'].fillna(False).sum())}")


if __name__ == "__main__":
    main()
