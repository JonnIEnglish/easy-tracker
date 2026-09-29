# EasyETFs Holdings Tracker

**Live dashboard: https://jonnienglish.github.io/easy-tracker/**

This repository captures historical holdings CSVs for three EasyETFs funds and publishes a GitHub Pages dashboard: NAV and premium/discount history, risk stats, holdings changes, return attribution, and look-through exposure for the fund-of-funds.

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
6. Combines hourly NAV and market price observations into `data/nav_price_history.csv`.
7. Builds `site/data.json` (analytics in `scripts/analytics.py`) for the static dashboard in `site/index.html`.
8. Commits any changed data and deploys `site/` to GitHub Pages.

## Dashboard

- **Overview**: fund cards, rebased NAV comparison, premium/discount, auto-generated takeaways, 14-day change feed, EASYBF look-through (sees through the AI/EGE sleeves), EASYGE/EASYAI overlap, pipeline health.
- **Per fund**: NAV vs market price, premium/discount percentile, volatility/drawdown, concentration, 30-day contribution estimate, sortable holdings table (weight deltas, stock moves, holding tenure, trend), top-holding weight history, turnover, and a 90-day activity log.

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
