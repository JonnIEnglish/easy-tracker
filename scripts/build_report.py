from __future__ import annotations

from pathlib import Path

import pandas as pd


def main() -> None:
    summary_path = Path("data/analytics/latest_summary.csv")
    if not summary_path.exists():
        raise FileNotFoundError("Run analytics first")

    summary = pd.read_csv(summary_path).iloc[0]
    date_str = str(summary["latest_snapshot_date"])
    md = Path("reports/latest_report.md")
    html = Path("reports/latest_report.html")
    archive = Path(f"reports/archive/report_{date_str[:7].replace('-', '_')}.md")

    content = f"""# EASYGE Monthly Holdings Report

Snapshot date: {date_str}

## Summary

- Holdings: {int(summary['num_holdings'])}
- Cash: {float(summary['cash_weight']):.2f}%
- Top 10: {float(summary['top_10_weight']):.2f}%
- Top 20: {float(summary['top_20_weight']):.2f}%

Dashboard: (configure GitHub Pages URL)
"""
    md.write_text(content, encoding="utf-8")
    html.write_text(f"<html><body><pre>{content}</pre></body></html>", encoding="utf-8")
    archive.write_text(content, encoding="utf-8")
    print("Monthly report built.")


if __name__ == "__main__":
    main()
