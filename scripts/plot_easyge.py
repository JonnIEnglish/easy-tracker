import os
from pathlib import Path

import pandas as pd

DATA = Path("data/holdings_history.csv")
CHART_DIR = Path("charts")
CACHE_DIR = Path(".cache/matplotlib")
XDG_CACHE_DIR = Path(".cache")

os.environ.setdefault("MPLCONFIGDIR", str(CACHE_DIR.resolve()))
os.environ.setdefault("XDG_CACHE_HOME", str(XDG_CACHE_DIR.resolve()))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
XDG_CACHE_DIR.mkdir(parents=True, exist_ok=True)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main() -> None:
    if not DATA.exists():
        raise FileNotFoundError(f"Missing {DATA}. Run scripts/fetch_easyge.py first.")

    CHART_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(DATA)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])

    latest_date = df["snapshot_date"].max()
    latest = df[df["snapshot_date"] == latest_date].sort_values("weight", ascending=False)
    top_names = latest.head(10)["instrument"].tolist()

    pivot = (
        df[df["instrument"].isin(top_names)]
        .pivot_table(
            index="snapshot_date",
            columns="instrument",
            values="weight",
            aggfunc="sum",
        )
        .fillna(0)
        .sort_index()
    )

    ax = pivot.plot.area(figsize=(12, 7))
    ax.set_title("EASYGE Top 10 Holdings Over Time")
    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio Weight (%)")
    ax.legend(loc="center left", bbox_to_anchor=(1.0, 0.5))
    plt.tight_layout()
    plt.savefig(CHART_DIR / "top_holdings_area.png", dpi=160)
    plt.close()

    latest.head(20).plot.barh(
        x="instrument",
        y="weight",
        figsize=(10, 8),
        legend=False,
    )
    plt.title(f"EASYGE Top 20 Holdings — {latest_date.date()}")
    plt.xlabel("Portfolio Weight (%)")
    plt.ylabel("")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig(CHART_DIR / "latest_top20.png", dpi=160)
    plt.close()

    print(f"Saved charts to {CHART_DIR}")


if __name__ == "__main__":
    main()
