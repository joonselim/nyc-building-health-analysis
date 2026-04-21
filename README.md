# NYC Building Health Analysis

Prepared as part of a portfolio project for applying to **Daisy** (Product Operations internship).

## What this is

Daisy grows by acquiring new buildings. A building switches management companies when the board decides the current company isn't doing a good enough job.

This project answers: **which buildings in NYC are most likely to be unhappy with their current management — and where are they?**

Using NYC's HPD (Housing Preservation and Development) public violation records, I built a scoring model that ranks buildings by management health and visualizes the results on an interactive map.

## What's inside

- `part_a_fetch.py` / `part_a_load_csv.py` — data ingestion from NYC Open Data (HPD violations)
- `part_a_map.py` — prospect map generation (interactive HTML map)
- `part_b_visualize.py` — health score model and visualizations
- `build_report.py` — final report builder
- `report.html` — final report (viewable via GitHub Pages)
- `data/` — processed datasets

## How to run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt  # if added
python part_a_load_csv.py
python part_a_map.py
python part_b_visualize.py
python build_report.py
```
