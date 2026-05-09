from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from scripts.utils import load_funds_config, read_csv_if_exists, utc_now_iso, write_csv

MARKET_PRICE_HISTORY_PATH = Path("data/market_price_history.csv")
MARKET_SOURCE = "yfinance"
DEFAULT_SUFFIX = ".JO"


@dataclass(frozen=True)
class FundTicker:
    fund_code: str
    ticker: str


def configured_fund_tickers() -> list[FundTicker]:
    cfg = load_funds_config()
    return [
        FundTicker(
            fund_code=str(row["code"]),
            ticker=str(row.get("market_ticker") or f"{row['code']}{DEFAULT_SUFFIX}"),
        )
        for row in cfg["funds"]
    ]


def parse_price_observation(
    fund_code: str,
    ticker: str,
    price: float | int | None,
    price_timestamp: str | None,
    captured_at_utc: str,
) -> dict[str, Any] | None:
    if price is None or not pd.notna(price):
        return None
    try:
        numeric = float(price)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    return {
        "fund_code": fund_code,
        "ticker": ticker,
        "price": numeric,
        "source": MARKET_SOURCE,
        "price_at_utc": price_timestamp,
        "captured_at_utc": captured_at_utc,
    }


def fetch_latest_price(ticker: str) -> tuple[float | None, str | None]:
    try:
        history = yf.Ticker(ticker).history(period="5d", interval="1d", auto_adjust=False)
    except Exception:
        return None, None
    if history.empty:
        return None, None
    last_row = history.dropna(subset=["Close"]).tail(1)
    if last_row.empty:
        return None, None
    close_value = float(last_row["Close"].iloc[0])
    last_index = last_row.index[-1]
    timestamp = pd.Timestamp(last_index)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return close_value, timestamp.tz_convert("UTC").isoformat().replace("+00:00", "Z")


def main() -> None:
    captured_at_utc = utc_now_iso()
    history = read_csv_if_exists(MARKET_PRICE_HISTORY_PATH)
    rows: list[dict[str, Any]] = []
    for entry in configured_fund_tickers():
        try:
            price, price_timestamp = fetch_latest_price(entry.ticker)
        except Exception as exc:
            print(f"{entry.fund_code}: failed to fetch market price ({exc})")
            continue
        observation = parse_price_observation(entry.fund_code, entry.ticker, price, price_timestamp, captured_at_utc)
        if observation is None:
            print(f"{entry.fund_code}: market price unavailable for {entry.ticker}")
            continue
        rows.append(observation)
        print(f"{entry.fund_code}: captured market price {observation['price']} ({entry.ticker})")

    if not rows:
        print("No market price observations captured.")
        return

    new_rows = pd.DataFrame(rows)
    if history.empty:
        merged = new_rows
    else:
        merged = pd.concat([history, new_rows], ignore_index=True)
    merged = merged.drop_duplicates(subset=["fund_code", "ticker", "price_at_utc", "price"], keep="last")
    merged = merged.sort_values(["fund_code", "captured_at_utc"])
    write_csv(merged, MARKET_PRICE_HISTORY_PATH)
    print(f"Wrote {MARKET_PRICE_HISTORY_PATH} with {len(new_rows)} new row(s).")


if __name__ == "__main__":
    main()
