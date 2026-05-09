# EasyETFs Holdings Tracker

This repository captures historical holdings CSVs for three EasyETFs funds and publishes a GitHub Pages viewer for the latest holdings plus 1d, 7d, and 30d stock performance.

Tracked funds:

- `easyge` / `EASYGE`: EasyETFs Global Equity Actively Managed ETF
- `easyai` / `EASYAI`: EasyETFs AI World Actively Managed ETF
- `easybalanced` / `EASYBF`: EasyETFs Balanced Actively Managed ETF

## How It Works

The hourly GitHub Action:

1. Looks back for the latest holdings CSV for each fund.
2. Saves new raw CSVs under `data/raw/<fund>/`.
3. Appends normalized rows to `data/holdings_history.csv`.
4. Builds `site/data.json` for the static dashboard.
5. Commits any changed data and deploys `site/` to GitHub Pages.

No PNG plots are generated.

## Local Commands

```bash
pip install -r requirements.txt
python -m scripts.fetch_holdings
python -m scripts.build_site_data
python -m pytest
```
