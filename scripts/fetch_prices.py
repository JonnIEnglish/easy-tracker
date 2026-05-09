from __future__ import annotations

from pathlib import Path

import pandas as pd


def fetch_yfinance_prices(ticker: str, start: str, end: str) -> pd.DataFrame:
    import yfinance as yf

    df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
    if df.empty:
        return pd.DataFrame(columns=["date", "close"])

    # yfinance may return MultiIndex columns (e.g. ("Close", "AAPL")).
    out = df.reset_index().copy()
    out.columns = [
        "_".join(str(part) for part in col if str(part).strip()) if isinstance(col, tuple) else str(col)
        for col in out.columns
    ]

    date_col = "Date" if "Date" in out.columns else ("date" if "date" in out.columns else None)
    close_candidates = [f"Close_{ticker}", "Close", f"close_{ticker}", "close"]
    close_col = next((c for c in close_candidates if c in out.columns), None)

    if not date_col or not close_col:
        return pd.DataFrame(columns=["date", "close"])

    out = out[[date_col, close_col]].rename(columns={date_col: "date", close_col: "close"})
    out["date"] = pd.to_datetime(out["date"]).dt.date.astype(str)
    return out


def fetch_easyge_price() -> pd.DataFrame:
    """TODO: implement JSE/EasyEquities provider. Placeholder in v1."""
    return pd.DataFrame(columns=["date", "ticker", "currency", "close", "source", "updated_at_utc"])


def fetch_fx_prices(start: str, end: str) -> pd.DataFrame:
    fx = fetch_yfinance_prices("ZAR=X", start, end)
    if fx.empty:
        return pd.DataFrame(columns=["date", "pair", "close", "source"])
    fx["pair"] = "USD/ZAR"
    fx["source"] = "yfinance"
    return fx


def main() -> None:
    holdings_path = Path("data/holdings_history.csv")
    ticker_map_path = Path("config/ticker_map.csv")
    benchmarks_path = Path("config/benchmarks.csv")
    prices_dir = Path("data/prices")
    reports_dir = Path("reports")
    prices_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    if not holdings_path.exists() or not ticker_map_path.exists():
        print("Price fetch skipped: holdings or ticker map missing.")
        return

    holdings = pd.read_csv(holdings_path)
    ticker_map = pd.read_csv(ticker_map_path)
    non_cash = holdings[~holdings["instrument"].str.contains("CASH", case=False, na=False)]
    inst = sorted(non_cash["instrument"].dropna().unique())

    mapped = ticker_map[ticker_map["active"].astype(str).str.lower() == "true"]["instrument"].tolist()
    missing = sorted(set(inst) - set(mapped))
    if missing:
        Path("reports/unmapped_instruments.md").write_text(
            "# Unmapped Instruments\n\n" + "\n".join(f"- {x}" for x in missing) + "\n", encoding="utf-8"
        )

    start = str(pd.to_datetime(holdings["snapshot_date"]).min().date())
    end = str(pd.Timestamp.today().date())

    stock_rows = []
    m = ticker_map[ticker_map["instrument"].isin(inst) & ticker_map["yfinance_ticker"].notna()].copy()
    for row in m.to_dict(orient="records"):
        px = fetch_yfinance_prices(row["yfinance_ticker"], start, end)
        for p in px.to_dict(orient="records"):
            stock_rows.append(
                {
                    "date": p["date"],
                    "instrument": row["instrument"],
                    "ticker": row["ticker"],
                    "yfinance_ticker": row["yfinance_ticker"],
                    "currency": row["currency"],
                    "close": p["close"],
                    "source": "yfinance",
                    "updated_at_utc": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            )
    pd.DataFrame(stock_rows).to_csv(prices_dir / "stock_prices.csv", index=False)

    if benchmarks_path.exists():
        b = pd.read_csv(benchmarks_path)
        b_rows = []
        for row in b.to_dict(orient="records"):
            px = fetch_yfinance_prices(row["yfinance_ticker"], start, end)
            for p in px.to_dict(orient="records"):
                b_rows.append(
                    {
                        "date": p["date"],
                        "name": row["name"],
                        "ticker": row["ticker"],
                        "yfinance_ticker": row["yfinance_ticker"],
                        "currency": row["currency"],
                        "close": p["close"],
                        "source": "yfinance",
                        "updated_at_utc": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                )
        pd.DataFrame(b_rows).to_csv(prices_dir / "benchmark_prices.csv", index=False)

    fetch_fx_prices(start, end).to_csv(prices_dir / "fx_prices.csv", index=False)
    fetch_easyge_price().to_csv(prices_dir / "easyge_prices.csv", index=False)

    print("Price fetch complete (with graceful placeholders where needed).")


if __name__ == "__main__":
    main()
