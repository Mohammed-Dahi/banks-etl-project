# 🏦 Top 10 Largest Banks — ETL Pipeline

An automated data engineering pipeline that extracts, transforms, and loads
the world's top 10 banks ranked by market capitalisation.

## Features
- **Extract** — Scrapes Wikipedia (Wayback Machine archive)
- **Transform** — Converts USD → GBP, EUR, INR using live exchange rates
- **Load** — Saves results to CSV and SQLite database
- **Logging** — Timestamped log file for every pipeline stage

## Tech Stack
Python · Pandas · BeautifulSoup · SQLite · Requests

## Output Columns
`Name` | `MC_USD_Billion` | `MC_GBP_Billion` | `MC_EUR_Billion` | `MC_INR_Billion`

## How to Run
```bash
pip install requests beautifulsoup4 pandas
python banks_project.py
```
