from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

os.environ.setdefault("MPLCONFIGDIR", str(Path(".cache/mpl").resolve()))
Path(".cache/mpl").mkdir(parents=True, exist_ok=True)
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HOLDINGS = Path("data/holdings_history.csv")
ANALYTICS = Path("data/analytics")
OUT = Path("site/charts")


def save_placeholder(path: Path, title: str) -> None:
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.text(0.5, 0.5, "Insufficient data", ha="center", va="center")
    ax.set_title(title)
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def main() -> None:
    if not HOLDINGS.exists():
        raise FileNotFoundError("Missing holdings data")

    OUT.mkdir(parents=True, exist_ok=True)
    h = pd.read_csv(HOLDINGS)
    h["snapshot_date"] = pd.to_datetime(h["snapshot_date"])

    latest_dt = h["snapshot_date"].max()
    latest = h[h["snapshot_date"] == latest_dt].sort_values("weight", ascending=False)

    fig, ax = plt.subplots(figsize=(10, 8))
    top20 = latest.head(20).iloc[::-1]
    ax.barh(top20["instrument"], top20["weight"])
    ax.set_title(f"EASYGE Top 20 Holdings - {latest_dt.date().isoformat()}")
    ax.set_xlabel("Weight (%)")
    fig.tight_layout()
    fig.savefig(OUT / "latest_top20.png", dpi=160)
    plt.close(fig)

    ch = pd.read_csv(ANALYTICS / "concentration_history.csv") if (ANALYTICS / "concentration_history.csv").exists() else pd.DataFrame()
    if not ch.empty:
        ch["snapshot_date"] = pd.to_datetime(ch["snapshot_date"])
        fig, ax = plt.subplots(figsize=(10, 6))
        for col in ["top_1_weight", "top_5_weight", "top_10_weight", "top_20_weight", "cash_weight"]:
            ax.plot(ch["snapshot_date"], ch[col], label=col.replace("_", " "))
        ax.legend()
        ax.set_title("Concentration History")
        ax.set_ylabel("Weight (%)")
        fig.tight_layout()
        fig.savefig(OUT / "concentration.png", dpi=160)
        plt.close(fig)
    else:
        save_placeholder(OUT / "concentration.png", "Concentration History")

    top_names = latest.head(10)["instrument"].tolist()
    pivot = h[h["instrument"].isin(top_names)].pivot_table(index="snapshot_date", columns="instrument", values="weight", aggfunc="sum").fillna(0)
    if len(pivot.index) >= 1:
        fig, ax = plt.subplots(figsize=(12, 7))
        ax.stackplot(pivot.index, [pivot[c] for c in pivot.columns], labels=pivot.columns)
        ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5))
        ax.set_title("Top 10 Holdings Over Time")
        ax.set_ylabel("Weight (%)")
        fig.tight_layout()
        fig.savefig(OUT / "top_holdings_area.png", dpi=160)
        plt.close(fig)
    else:
        save_placeholder(OUT / "top_holdings_area.png", "Top 10 Holdings")

    cash = h[h["instrument"].str.contains("CASH", case=False, na=False)].copy()
    if not cash.empty:
        cp = cash.pivot_table(index="snapshot_date", columns="instrument", values="weight", aggfunc="sum").fillna(0)
        cp["total_cash"] = cp.sum(axis=1)
        fig, ax = plt.subplots(figsize=(10, 5))
        for c in cp.columns:
            ax.plot(cp.index, cp[c], label=c)
        ax.legend()
        ax.set_title("Cash Exposure")
        ax.set_ylabel("Weight (%)")
        fig.tight_layout()
        fig.savefig(OUT / "cash_exposure.png", dpi=160)
        plt.close(fig)
    else:
        save_placeholder(OUT / "cash_exposure.png", "Cash Exposure")

    turn = pd.read_csv(ANALYTICS / "turnover_history.csv") if (ANALYTICS / "turnover_history.csv").exists() else pd.DataFrame()
    if not turn.empty:
        turn["snapshot_date"] = pd.to_datetime(turn["snapshot_date"])
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(turn["snapshot_date"].dt.strftime("%Y-%m-%d"), turn["turnover_estimate"])
        ax.set_title("Turnover Estimate")
        ax.set_ylabel("Turnover")
        plt.xticks(rotation=45, ha="right")
        fig.tight_layout()
        fig.savefig(OUT / "turnover.png", dpi=160)
        plt.close(fig)
    else:
        save_placeholder(OUT / "turnover.png", "Turnover")

    changes = pd.read_csv(ANALYTICS / "monthly_changes.csv") if (ANALYTICS / "monthly_changes.csv").exists() else pd.DataFrame()
    if not changes.empty:
        latest_change_dt = changes["snapshot_date"].max()
        c = changes[changes["snapshot_date"] == latest_change_dt].copy()
        c = c.sort_values("weight_change")
        fig, ax = plt.subplots(figsize=(10, 8))
        subset = pd.concat([c.head(10), c.tail(10)])
        ax.barh(subset["instrument"], subset["weight_change"])
        ax.set_title(f"Latest Weight Changes ({latest_change_dt})")
        ax.set_xlabel("Weight Change (pp)")
        fig.tight_layout()
        fig.savefig(OUT / "weight_changes.png", dpi=160)
        plt.close(fig)
    else:
        save_placeholder(OUT / "weight_changes.png", "Weight Changes")

    theme = pd.read_csv(ANALYTICS / "theme_exposure.csv") if (ANALYTICS / "theme_exposure.csv").exists() else pd.DataFrame()
    if not theme.empty:
        theme["snapshot_date"] = pd.to_datetime(theme["snapshot_date"])
        tp = theme.pivot_table(index="snapshot_date", columns="theme", values="weight", aggfunc="sum").fillna(0)
        fig, ax = plt.subplots(figsize=(12, 7))
        ax.stackplot(tp.index, [tp[c] for c in tp.columns], labels=tp.columns)
        ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5))
        ax.set_title("Theme Exposure")
        ax.set_ylabel("Weight (%)")
        fig.tight_layout()
        fig.savefig(OUT / "theme_exposure.png", dpi=160)
        plt.close(fig)
    else:
        save_placeholder(OUT / "theme_exposure.png", "Theme Exposure")

    for name, title in [
        ("easyge_vs_estimated_basket.png", "EASYGE vs Estimated Basket"),
        ("decision_scorecard.png", "Decision Scorecard"),
    ]:
        save_placeholder(OUT / name, title)

    print("Charts generated.")


if __name__ == "__main__":
    main()
