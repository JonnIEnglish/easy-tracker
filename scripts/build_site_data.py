from __future__ import annotations

import json
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd

from scripts.utils import load_funds_config, read_csv_if_exists, utc_now_iso

HISTORY_PATH = Path("data/holdings_history.csv")
TICKER_MAP_PATH = Path("config/ticker_map.csv")
SITE_DATA_PATH = Path("site/data.json")


def pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return ((current / previous) - 1) * 100


def latest_holdings(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return history
    history = history.copy()
    history["captured_at_utc"] = pd.to_datetime(history["captured_at_utc"], utc=True)
    idx = history.groupby("fund_code")["captured_at_utc"].idxmax()
    latest_capture = history.loc[idx, ["fund_code", "captured_at_utc"]]
    return history.merge(latest_capture, on=["fund_code", "captured_at_utc"], how="inner")


def fetch_price_history(tickers: list[str]) -> dict[str, pd.DataFrame]:
    if not tickers:
        return {}
    import yfinance as yf

    prices: dict[str, pd.DataFrame] = {}
    for ticker in tickers:
        try:
            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                df = yf.download(ticker, period="45d", interval="1d", auto_adjust=True, progress=False, threads=False)
        except Exception:
            continue
        if df.empty:
            continue
        frame = df.reset_index()
        frame.columns = [
            "_".join(str(part) for part in col if str(part).strip()) if isinstance(col, tuple) else str(col)
            for col in frame.columns
        ]
        date_col = "Date" if "Date" in frame.columns else "date"
        close_col = next((col for col in frame.columns if col == "Close" or col.startswith("Close_")), None)
        if not close_col:
            continue
        out = frame[[date_col, close_col]].rename(columns={date_col: "date", close_col: "close"})
        out["date"] = pd.to_datetime(out["date"]).dt.date
        out["close"] = pd.to_numeric(out["close"], errors="coerce")
        out = out.dropna(subset=["close"]).sort_values("date")
        if not out.empty:
            prices[ticker] = out
    return prices


def close_on_or_before(prices: pd.DataFrame, target: pd.Timestamp) -> float | None:
    date_value = target.date()
    subset = prices[prices["date"] <= date_value]
    if subset.empty:
        return None
    return float(subset.iloc[-1]["close"])


def performance_for(prices: pd.DataFrame | None) -> dict[str, float | None]:
    if prices is None or prices.empty:
        return {"d1": None, "d7": None, "d30": None}
    current_date = pd.Timestamp(prices.iloc[-1]["date"])
    current = float(prices.iloc[-1]["close"])
    return {
        "d1": pct_change(current, close_on_or_before(prices, current_date - pd.Timedelta(days=1))),
        "d7": pct_change(current, close_on_or_before(prices, current_date - pd.Timedelta(days=7))),
        "d30": pct_change(current, close_on_or_before(prices, current_date - pd.Timedelta(days=30))),
    }


def build_payload() -> dict[str, Any]:
    cfg = load_funds_config()
    funds_cfg = {row["code"]: row for row in cfg["funds"]}
    history = latest_holdings(read_csv_if_exists(HISTORY_PATH))
    ticker_map = read_csv_if_exists(TICKER_MAP_PATH)

    ticker_by_instrument: dict[str, str] = {}
    if not ticker_map.empty:
        active = ticker_map[ticker_map["yfinance_ticker"].notna()].copy()
        active["yfinance_ticker"] = active["yfinance_ticker"].astype(str).str.strip()
        active = active[active["yfinance_ticker"] != ""]
        ticker_by_instrument = dict(zip(active["instrument"].astype(str), active["yfinance_ticker"]))

    tickers = sorted({ticker_by_instrument.get(str(x)) for x in history.get("instrument", pd.Series(dtype=str)).dropna() if ticker_by_instrument.get(str(x))})
    price_history = fetch_price_history(tickers)
    performance = {ticker: performance_for(df) for ticker, df in price_history.items()}

    funds: list[dict[str, Any]] = []
    for code, fund_cfg in funds_cfg.items():
        rows = history[history["fund_code"].astype(str) == code].copy() if not history.empty else pd.DataFrame()
        rows = rows.sort_values("weight", ascending=False) if not rows.empty else rows
        holdings = []
        for row in rows.to_dict(orient="records"):
            instrument = str(row["instrument"])
            ticker = ticker_by_instrument.get(instrument)
            holdings.append(
                {
                    "instrument": instrument,
                    "currency": row["currency"],
                    "weight": float(row["weight"]),
                    "ticker": ticker,
                    "performance": performance.get(ticker, {"d1": None, "d7": None, "d30": None}),
                }
            )
        funds.append(
            {
                "code": code,
                "slug": fund_cfg["slug"],
                "name": fund_cfg["name"],
                "instrument_page": fund_cfg.get("instrument_page"),
                "snapshot_date": str(rows["snapshot_date"].iloc[0]) if not rows.empty else None,
                "captured_at_utc": rows["captured_at_utc"].iloc[0].isoformat().replace("+00:00", "Z") if not rows.empty else None,
                "holdings_count": int(len(rows)),
                "total_weight": round(float(rows["weight"].sum()), 4) if not rows.empty else None,
                "holdings": holdings,
            }
        )

    return {"generated_at_utc": utc_now_iso(), "funds": funds}


def main() -> None:
    SITE_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = build_payload()
    SITE_DATA_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {SITE_DATA_PATH} for {len(payload['funds'])} funds.")


if __name__ == "__main__":
    main()
