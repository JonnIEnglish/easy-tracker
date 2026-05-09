# EASYGE Tracker

Track monthly holdings snapshots for the EasyETFs Global Equity Actively Managed ETF (EASYGE), store a local history, and generate charts.

## Project Structure

```text
easyge-tracker/
  data/
    raw/
    holdings_history.csv
  charts/
  scripts/
    fetch_easyge.py
    plot_easyge.py
  .github/
    workflows/
      monthly.yml
```

## Local Usage

1. Create a virtual environment (optional but recommended).
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Fetch latest holdings snapshot:

   ```bash
   python scripts/fetch_easyge.py
   ```

4. Generate charts:

   ```bash
   python scripts/plot_easyge.py
   ```

## Publish To GitHub

1. Create an empty GitHub repository (for example: `jonno/easy-tracker`).
2. Add your remote and push:

   ```bash
   git remote add origin git@github.com:jonno/easy-tracker.git
   git branch -M main
   git add .
   git commit -m "Initial EASYGE tracker"
   git push -u origin main
   ```

## Automated Updates (GitHub Actions)

- Workflow file: `.github/workflows/monthly.yml`
- Trigger:
  - every 6 hours (UTC)
  - manual run from GitHub Actions (`workflow_dispatch`)
- Behavior:
  - fetches latest EASYGE data
  - regenerates charts
  - commits and pushes only when files changed

## Notes

- The fetch script checks today first, then walks backward up to 10 days for the latest available dated CSV.
- The script stores raw CSV snapshots in `data/raw/` and appends normalized rows to `data/holdings_history.csv`.
