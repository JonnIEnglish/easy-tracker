from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

BASE_URL = "https://easyetf-constituents.s3.us-west-2.amazonaws.com/EASYGE_PCF_ZAR_{date}.csv"

RAW_DIR = Path("data/raw")
HISTORY_FILE = Path("data/holdings_history.csv")


def _normalize_col(col: str) -> str:
    return col.strip().lower().replace(" ", "_")


def _first_match(columns: Iterable[str], candidates: set[str]) -> str | None:
    for c in columns:
        if c in candidates:
            return c
    return None


def fetch_latest(max_lookback_days: int = 10) -> tuple[str, Path]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    for i in range(max_lookback_days + 1):
        d = date.today() - timedelta(days=i)
        yyyymmdd = d.strftime("%Y%m%d")
        url = BASE_URL.format(date=yyyymmdd)

        response = requests.get(url, timeout=20)
        if response.status_code == 200 and response.text.strip():
            raw_path = RAW_DIR / f"EASYGE_PCF_ZAR_{yyyymmdd}.csv"
            raw_path.write_text(response.text, encoding="utf-8")
            return yyyymmdd, raw_path

    raise RuntimeError("Could not find a recent EASYGE holdings CSV in lookback window.")


def normalize_csv(snapshot_date: str, raw_path: Path) -> pd.DataFrame:
    df = pd.read_csv(raw_path)
    df.columns = [_normalize_col(c) for c in df.columns]

    instrument_col = _first_match(
        df.columns,
        {
            "instrument",
            "security",
            "name",
            "holding",
            "description",
            "instrument_name",
            "security_name",
        },
    )
    currency_col = _first_match(df.columns, {"currency", "ccy", "trading_currency"})
    weight_col = _first_match(
        df.columns,
        {
            "weight",
            "weight_%",
            "weighting",
            "portfolio_weight",
            "holding_weight",
            "percentage",
        },
    )

    if not instrument_col or not currency_col or not weight_col:
        raise ValueError(
            "CSV format changed. "
            f"Found columns={df.columns.tolist()}, "
            f"instrument={instrument_col}, currency={currency_col}, weight={weight_col}"
        )

    out = df[[instrument_col, currency_col, weight_col]].copy()
    out.columns = ["instrument", "currency", "weight"]
    out["snapshot_date"] = pd.to_datetime(snapshot_date, format="%Y%m%d").date()

    out["weight"] = (
        out["weight"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    out["weight"] = pd.to_numeric(out["weight"], errors="coerce")
    out = out.dropna(subset=["instrument", "currency", "weight"])

    return out[["snapshot_date", "instrument", "currency", "weight"]]


def merge_history(new_df: pd.DataFrame) -> pd.DataFrame:
    if HISTORY_FILE.exists():
        hist = pd.read_csv(HISTORY_FILE)
        hist["snapshot_date"] = pd.to_datetime(hist["snapshot_date"]).dt.date
        combined = pd.concat([hist, new_df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["snapshot_date", "instrument", "currency"],
            keep="last",
        )
    else:
        combined = new_df

    combined = combined.sort_values(["snapshot_date", "weight"], ascending=[True, False])
    return combined


def main() -> None:
    snapshot_date, raw_path = fetch_latest()
    new_df = normalize_csv(snapshot_date, raw_path)
    combined = merge_history(new_df)

    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(HISTORY_FILE, index=False)
    print(f"Saved snapshot {snapshot_date} with {len(new_df)} holdings.")


if __name__ == "__main__":
    main()
