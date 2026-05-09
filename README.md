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
4. Captures published NAV observations into `data/nav_history.csv`.
5. Captures latest public ETF market prices into `data/market_price_history.csv`.
6. Builds `site/data.json` for the static dashboard.
7. Commits any changed data and deploys `site/` to GitHub Pages.

No PNG plots are generated.

## Live Quote Override Decision

This dashboard remains strictly automated.

- Premium/discount to NAV is calculated from published NAV and latest available public market prices.
- No manual EasyEquities live quote entry is supported in the dashboard.
- This avoids introducing logged-in, user-specific quote flows into the automated hourly pipeline.

## Local Commands

```bash
pip install -r requirements.txt
python -m scripts.fetch_holdings
python -m scripts.build_site_data
python -m pytest
```
