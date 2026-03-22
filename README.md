# 🇨🇦 Government of Canada Contract Data Analysis

This repository contains Python-based analysis of Government of Canada procurement data published through the Proactive Disclosure of Contracts initiative, covering contracts over $10,000.

The analysis focuses on data spanning multiple years (1999–2026), enabling both long-term trend analysis and recent procurement insights.

The project explores how public funds are distributed across vendors, organizations, and contract categories using data cleaning, exploratory analysis, and visualization.

---

## 🔗 Data Source

Data is sourced from the Government of Canada Open Data portal:

* Contracts over $10,000 (Proactive Disclosure)
* [https://search.open.canada.ca/contracts/download/a6537aa3-39b0-4e87-ab96-2c859bb1aa8d](https://search.open.canada.ca/contracts/download/a6537aa3-39b0-4e87-ab96-2c859bb1aa8d)

These datasets include contract-level details such as vendor, value, organization, commodity, and contract date.

---

## 🔍 Overview

Government procurement represents a significant portion of public spending, but patterns in vendor concentration and contract distribution are not immediately visible.

This project transforms raw contract data into structured insights to understand:

* Spending distribution across contract values
* Vendor concentration and market structure
* Category mix across different contract sizes
* Trends in procurement activity over time

---

## 📊 Key Insights

* **Vendor concentration (Pareto pattern)**
  A small number of vendors account for a large share of total contract value.

* **Long-tail contract distribution**
  Most contracts are low-value (< $100K), while a small number of high-value contracts drive overall spend.

* **Category dynamics**
  Goods and services dominate lower-value contracts; higher-value contracts are fewer and more specialized.

* **Organizational concentration**
  A limited number of departments account for a significant portion of procurement activity.

* **Agreement usage patterns**
  A small number of agreements are repeatedly referenced across contracts.

* **Temporal trends**
  Contract volume and value evolve over time with observable structural patterns.

---

## ⚙️ Tech Stack

* Python (pandas, matplotlib)
* Jupyter Notebooks
* Data cleaning and feature engineering pipelines
* Visualization-focused analysis

---

## 🧠 Methodology

* Data ingestion from open government sources
* Cleaning and normalization of vendor, date, and category fields
* Feature engineering (value bands, classifications, time aggregation)
* Analytical modeling and visualization (Pareto, distribution, time-series)
* Export of curated datasets and charts

---

## 📁 Repository Structure

```text
├── LICENSE
├── README.md
├── canada-gov-contracts-extraction.ipynb   # main analysis notebook
├── data
│   ├── raw_data                            # source data (ignored in Git)
│   │   └── contracts_above_10k.csv
│   ├── us_goods_contracts_2023_2026.csv    # curated dataset (goods)
│   └── us_services_contracts_2023_2026.csv # curated dataset (services)
├── docs                                    # documentation (optional)
├── notebooks
│   └── canada-gov-contracts-extraction.ipynb
├── output
│   └── vendor_concentration_pareto.png     # generated visualizations
├── pyproject.toml                          # project dependencies
├── uv.lock                                 # reproducible environment
```

---

## 🎯 Purpose

This project provides a data-driven view of public procurement to better understand:

* Supplier concentration risk
* Spending patterns and distribution
* Procurement strategy opportunities
* Market structure of government contracting

