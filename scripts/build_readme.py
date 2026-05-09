from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

SUMMARY = Path("data/analytics/latest_summary.csv")
CHANGES = Path("data/analytics/monthly_changes.csv")
HOLDINGS = Path("data/holdings_history.csv")
OUT = Path("README.md")


def fmt_pct(x: float) -> str:
    return f"{x:.2f}%"


def main() -> None:
    if not SUMMARY.exists() or not HOLDINGS.exists():
        raise FileNotFoundError("Run analytics first.")

    summary = pd.read_csv(SUMMARY).iloc[0]
    holdings = pd.read_csv(HOLDINGS)
    latest_date = summary["latest_snapshot_date"]
    latest = holdings[holdings["snapshot_date"] == latest_date].sort_values("weight", ascending=False)

    turnover_val = "n/a"
    turn_path = Path("data/analytics/turnover_history.csv")
    if turn_path.exists():
        try:
            t = pd.read_csv(turn_path)
        except EmptyDataError:
            t = pd.DataFrame()
        if not t.empty:
            turnover_val = f"{float(t.iloc[-1]['turnover_estimate']):.2f}"

    lines = [
        "# EASYGE Active Holdings Tracker",
        "",
        "Automated tracker for EASYGE holdings, concentration, turnover, and active-management signals.",
        "",
        "Dashboard: (configure GitHub Pages URL)",
        "",
        f"Latest snapshot: {latest_date}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Holdings | {int(summary['num_holdings'])} |",
        f"| Cash | {fmt_pct(float(summary['cash_weight']))} |",
        f"| Top 1 weight | {fmt_pct(float(summary['top_1_weight']))} |",
        f"| Top 5 weight | {fmt_pct(float(summary['top_5_weight']))} |",
        f"| Top 10 weight | {fmt_pct(float(summary['top_10_weight']))} |",
        f"| Top 20 weight | {fmt_pct(float(summary['top_20_weight']))} |",
        f"| Estimated turnover | {turnover_val} |",
        "",
        "## Latest Top 20 Holdings",
        "",
        "![Latest Top 20](site/charts/latest_top20.png)",
        "",
        "## Concentration",
        "",
        "![Concentration](site/charts/concentration.png)",
        "",
        "## Theme Exposure",
        "",
        "![Theme Exposure](site/charts/theme_exposure.png)",
        "",
        "## Latest Full Holdings",
        "",
        "| Instrument | Currency | Weight |",
        "|---|---:|---:|",
    ]

    for _, row in latest.head(33).iterrows():
        lines.append(f"| {row['instrument']} | {row['currency']} | {float(row['weight']):.2f}% |")

    if CHANGES.exists():
        changes = pd.read_csv(CHANGES)
        if not changes.empty:
            latest_change_dt = changes["snapshot_date"].max()
            c = changes[changes["snapshot_date"] == latest_change_dt].copy()
            c = c.sort_values("weight_change", ascending=False)
            lines.extend([
                "",
                "## Latest Monthly Changes",
                "",
                "| Holding | Previous | Current | Change | Type |",
                "|---|---:|---:|---:|---|",
            ])
            for _, row in c.head(20).iterrows():
                lines.append(
                    f"| {row['instrument']} | {float(row['previous_weight']):.2f}% | {float(row['current_weight']):.2f}% | {float(row['weight_change']):+.2f} | {row['change_type']} |"
                )

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("README rebuilt.")


if __name__ == "__main__":
    main()
