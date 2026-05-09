from __future__ import annotations

import json
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd

from scripts.utils import load_funds_config, read_csv_if_exists, utc_now_iso, write_csv

HISTORY_PATH = Path("data/holdings_history.csv")
TICKER_MAP_PATH = Path("config/ticker_map.csv")
NAV_HISTORY_PATH = Path("data/nav_history.csv")
MARKET_PRICE_HISTORY_PATH = Path("data/market_price_history.csv")
NAV_PRICE_HISTORY_PATH = Path("data/nav_price_history.csv")
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


def classify_holding_change(previous_weight: float, current_weight: float) -> str:
    if previous_weight == 0 and current_weight > 0:
        return "added"
    if previous_weight > 0 and current_weight == 0:
        return "exited"
    if current_weight > previous_weight:
        return "increased"
    if current_weight < previous_weight:
        return "trimmed"
    return "unchanged"


def derive_monthly_holdings_changes(history: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if history.empty:
        return {}

    working = history.copy()
    working["captured_at_utc"] = pd.to_datetime(working["captured_at_utc"], utc=True)
    working["month"] = working["captured_at_utc"].dt.strftime("%Y-%m")

    latest_in_month = working.loc[
        working.groupby(["fund_code", "month"])["captured_at_utc"].transform("max") == working["captured_at_utc"]
    ].copy()

    results: dict[str, dict[str, Any]] = {}
    for fund_code, fund_rows in latest_in_month.groupby("fund_code"):
        months = sorted(fund_rows["month"].drop_duplicates())
        if len(months) < 2:
            results[str(fund_code)] = {
                "previous_month": None,
                "current_month": None,
                "changes": [],
            }
            continue

        previous_month = months[-2]
        current_month = months[-1]
        previous_rows = fund_rows[fund_rows["month"] == previous_month]
        current_rows = fund_rows[fund_rows["month"] == current_month]

        previous_weights = (
            previous_rows.groupby("instrument", as_index=False)["weight"].sum().rename(columns={"weight": "previous_weight"})
        )
        current_weights = (
            current_rows.groupby("instrument", as_index=False)["weight"].sum().rename(columns={"weight": "current_weight"})
        )

        merged = previous_weights.merge(current_weights, on="instrument", how="outer").fillna(0.0)
        merged["previous_weight"] = pd.to_numeric(merged["previous_weight"], errors="coerce").fillna(0.0)
        merged["current_weight"] = pd.to_numeric(merged["current_weight"], errors="coerce").fillna(0.0)
        merged["change_pp"] = merged["current_weight"] - merged["previous_weight"]
        merged["action"] = merged.apply(
            lambda row: classify_holding_change(float(row["previous_weight"]), float(row["current_weight"])),
            axis=1,
        )

        merged = merged.sort_values(["change_pp", "instrument"], ascending=[False, True]).reset_index(drop=True)
        changes = [
            {
                "instrument": str(row["instrument"]),
                "previous_weight": float(row["previous_weight"]),
                "current_weight": float(row["current_weight"]),
                "change_pp": float(row["change_pp"]),
                "action": str(row["action"]),
            }
            for row in merged.to_dict(orient="records")
        ]

        results[str(fund_code)] = {
            "previous_month": str(previous_month),
            "current_month": str(current_month),
            "changes": changes,
        }

    return results


def derive_monthly_holdings_history(history: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if history.empty:
        return {}

    working = history.copy()
    working["captured_at_utc"] = pd.to_datetime(working["captured_at_utc"], utc=True)
    working["month"] = working["captured_at_utc"].dt.strftime("%Y-%m")

    latest_in_month = working.loc[
        working.groupby(["fund_code", "month"])["captured_at_utc"].transform("max") == working["captured_at_utc"]
    ].copy()
    latest_in_month["weight"] = pd.to_numeric(latest_in_month["weight"], errors="coerce").fillna(0.0)

    results: dict[str, dict[str, Any]] = {}
    for fund_code, fund_rows in latest_in_month.groupby("fund_code"):
        months = sorted(fund_rows["month"].drop_duplicates())
        if not months:
            results[str(fund_code)] = {"months": [], "rows": []}
            continue

        grouped = (
            fund_rows.groupby(["instrument", "month"], as_index=False)["weight"]
            .sum()
            .pivot(index="instrument", columns="month", values="weight")
            .fillna(0.0)
        )
        grouped = grouped.reindex(columns=months, fill_value=0.0)

        if months:
            latest_month = months[-1]
            grouped = grouped.assign(
                _latest_weight=grouped[latest_month],
                _max_weight=grouped.max(axis=1),
            ).sort_values(["_latest_weight", "_max_weight"], ascending=[False, False])

        rows = []
        for instrument, row in grouped.iterrows():
            weights = [float(row[month]) for month in months]
            active_month_indexes = [idx for idx, value in enumerate(weights) if value > 0]
            rows.append(
                {
                    "instrument": str(instrument),
                    "weights": weights,
                    "active_months": int(len(active_month_indexes)),
                    "first_month": months[active_month_indexes[0]] if active_month_indexes else None,
                    "last_month": months[active_month_indexes[-1]] if active_month_indexes else None,
                }
            )

        results[str(fund_code)] = {
            "months": months,
            "rows": rows,
        }

    return results


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


def latest_nav_by_fund(nav_history: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if nav_history.empty:
        return {}
    working = nav_history.copy()
    working["nav_date"] = pd.to_datetime(working["nav_date"], errors="coerce")
    working["captured_at_utc"] = pd.to_datetime(working["captured_at_utc"], utc=True, errors="coerce")
    working = working.dropna(subset=["fund_code", "nav_zac", "nav_date", "captured_at_utc"])
    if working.empty:
        return {}
    working["nav_zac"] = pd.to_numeric(working["nav_zac"], errors="coerce")
    working = working.dropna(subset=["nav_zac"])
    working = working.sort_values(["fund_code", "nav_date", "captured_at_utc"], ascending=[True, False, False])
    latest = working.groupby("fund_code", as_index=False).head(1)
    return {
        str(row["fund_code"]): {
            "value_zac": float(row["nav_zac"]),
            "nav_date": row["nav_date"].date().isoformat(),
            "source_url": str(row["source_url"]),
            "captured_at_utc": row["captured_at_utc"].isoformat().replace("+00:00", "Z"),
        }
        for row in latest.to_dict(orient="records")
    }


def latest_market_price_by_fund(price_history: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if price_history.empty:
        return {}
    working = price_history.copy()
    working["captured_at_utc"] = pd.to_datetime(working["captured_at_utc"], utc=True, errors="coerce")
    working["price_at_utc"] = pd.to_datetime(working.get("price_at_utc"), utc=True, errors="coerce")
    working["price"] = pd.to_numeric(working["price"], errors="coerce")
    working = working.dropna(subset=["fund_code", "ticker", "price", "captured_at_utc"])
    if working.empty:
        return {}
    working["_price_sort"] = working["price_at_utc"].fillna(working["captured_at_utc"])
    working = working.sort_values(["fund_code", "_price_sort", "captured_at_utc"], ascending=[True, False, False])
    latest = working.groupby("fund_code", as_index=False).head(1)
    return {
        str(row["fund_code"]): {
            "ticker": str(row["ticker"]),
            "value_zac": float(row["price"]),
            "source": str(row.get("source") or ""),
            "price_at_utc": row["price_at_utc"].isoformat().replace("+00:00", "Z")
            if pd.notna(row["price_at_utc"])
            else None,
            "captured_at_utc": row["captured_at_utc"].isoformat().replace("+00:00", "Z"),
        }
        for row in latest.to_dict(orient="records")
    }


def estimate_premium_discount_to_nav(
    latest_nav: dict[str, Any] | None,
    latest_market_price: dict[str, Any] | None,
    near_nav_threshold_pct: float = 0.25,
) -> dict[str, Any]:
    if not latest_nav or not latest_market_price:
        return {
            "status": "n/a",
            "difference_zac": None,
            "difference_pct": None,
            "label": "n/a",
        }
    nav_value = float(latest_nav.get("value_zac")) if latest_nav.get("value_zac") is not None else None
    market_value = float(latest_market_price.get("value_zac")) if latest_market_price.get("value_zac") is not None else None
    if not nav_value or not market_value:
        return {
            "status": "n/a",
            "difference_zac": None,
            "difference_pct": None,
            "label": "n/a",
        }
    difference_zac = market_value - nav_value
    difference_pct = (difference_zac / nav_value) * 100 if nav_value else None
    if difference_pct is None:
        status = "n/a"
    elif abs(difference_pct) <= near_nav_threshold_pct:
        status = "near_nav"
    elif difference_pct > 0:
        status = "premium"
    else:
        status = "discount"
    return {
        "status": status,
        "difference_zac": float(difference_zac),
        "difference_pct": float(difference_pct) if difference_pct is not None else None,
        "label": "estimated premium/discount to NAV",
    }


def derive_nav_price_history(nav_history: pd.DataFrame, price_history: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "fund_code",
        "captured_hour_utc",
        "nav_zac",
        "nav_date",
        "nav_captured_at_utc",
        "market_ticker",
        "market_price_zac",
        "market_price_at_utc",
        "market_captured_at_utc",
        "difference_zac",
        "difference_pct",
        "status",
    ]
    if nav_history.empty and price_history.empty:
        return pd.DataFrame(columns=columns)

    nav = pd.DataFrame(columns=["fund_code", "captured_hour_utc", "nav_zac", "nav_date", "nav_captured_at_utc"])
    if not nav_history.empty:
        nav = nav_history.copy()
        nav["nav_captured_at_utc"] = pd.to_datetime(nav["captured_at_utc"], utc=True, errors="coerce")
        nav["captured_hour_utc"] = nav["nav_captured_at_utc"].dt.floor("h")
        nav["nav_zac"] = pd.to_numeric(nav["nav_zac"], errors="coerce")
        nav = nav.dropna(subset=["fund_code", "captured_hour_utc", "nav_zac"])
        nav = nav.sort_values(["fund_code", "captured_hour_utc", "nav_captured_at_utc"])
        nav = nav.groupby(["fund_code", "captured_hour_utc"], as_index=False).tail(1)
        nav = nav[["fund_code", "captured_hour_utc", "nav_zac", "nav_date", "nav_captured_at_utc"]]

    price = pd.DataFrame(
        columns=["fund_code", "captured_hour_utc", "market_ticker", "market_price_zac", "market_price_at_utc", "market_captured_at_utc"]
    )
    if not price_history.empty:
        price = price_history.copy()
        price["market_captured_at_utc"] = pd.to_datetime(price["captured_at_utc"], utc=True, errors="coerce")
        price["captured_hour_utc"] = price["market_captured_at_utc"].dt.floor("h")
        price["market_price_at_utc"] = pd.to_datetime(price.get("price_at_utc"), utc=True, errors="coerce")
        price["market_price_zac"] = pd.to_numeric(price["price"], errors="coerce")
        price = price.dropna(subset=["fund_code", "captured_hour_utc", "market_price_zac"])
        price = price.sort_values(["fund_code", "captured_hour_utc", "market_captured_at_utc"])
        price = price.groupby(["fund_code", "captured_hour_utc"], as_index=False).tail(1)
        price = price.rename(columns={"ticker": "market_ticker"})
        price = price[
            ["fund_code", "captured_hour_utc", "market_ticker", "market_price_zac", "market_price_at_utc", "market_captured_at_utc"]
        ]

    combined = nav.merge(price, on=["fund_code", "captured_hour_utc"], how="outer")
    if combined.empty:
        return pd.DataFrame(columns=columns)

    combined["difference_zac"] = combined["market_price_zac"] - combined["nav_zac"]
    combined["difference_pct"] = (combined["difference_zac"] / combined["nav_zac"]) * 100
    combined["status"] = combined["difference_pct"].map(
        lambda value: "n/a"
        if pd.isna(value)
        else ("near_nav" if abs(float(value)) <= 0.25 else ("premium" if float(value) > 0 else "discount"))
    )
    for col in ["captured_hour_utc", "nav_captured_at_utc", "market_price_at_utc", "market_captured_at_utc"]:
        combined[col] = pd.to_datetime(combined[col], utc=True, errors="coerce").map(
            lambda value: value.isoformat().replace("+00:00", "Z") if pd.notna(value) else None
        )
    combined = combined.sort_values(["fund_code", "captured_hour_utc"])
    return combined.reindex(columns=columns)


def nav_price_history_by_fund(history: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    if history.empty:
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for fund_code, rows in history.groupby("fund_code"):
        clean_rows = rows.sort_values("captured_hour_utc").where(pd.notna(rows), None)
        out[str(fund_code)] = [
            {
                "captured_hour_utc": row["captured_hour_utc"],
                "nav_zac": float(row["nav_zac"]) if row["nav_zac"] is not None else None,
                "market_price_zac": float(row["market_price_zac"]) if row["market_price_zac"] is not None else None,
                "difference_zac": float(row["difference_zac"]) if row["difference_zac"] is not None else None,
                "difference_pct": float(row["difference_pct"]) if row["difference_pct"] is not None else None,
                "status": row["status"],
            }
            for row in clean_rows.to_dict(orient="records")
        ]
    return out


def build_payload() -> dict[str, Any]:
    cfg = load_funds_config()
    funds_cfg = {row["code"]: row for row in cfg["funds"]}
    full_history = read_csv_if_exists(HISTORY_PATH)
    history = latest_holdings(full_history)
    monthly_changes_by_fund = derive_monthly_holdings_changes(full_history)
    monthly_history_by_fund = derive_monthly_holdings_history(full_history)
    nav_history = read_csv_if_exists(NAV_HISTORY_PATH)
    market_price_history = read_csv_if_exists(MARKET_PRICE_HISTORY_PATH)
    latest_navs = latest_nav_by_fund(nav_history)
    latest_market_prices = latest_market_price_by_fund(market_price_history)
    nav_price_history = derive_nav_price_history(nav_history, market_price_history)
    nav_price_history_series = nav_price_history_by_fund(nav_price_history)
    ticker_map = read_csv_if_exists(TICKER_MAP_PATH)

    ticker_by_instrument: dict[str, str] = {}
    if not ticker_map.empty:
        active = ticker_map[ticker_map["yfinance_ticker"].notna()].copy()
        active["yfinance_ticker"] = active["yfinance_ticker"].astype(str).str.strip()
        active = active[active["yfinance_ticker"] != ""]
        ticker_by_instrument = dict(zip(active["instrument"].astype(str), active["yfinance_ticker"]))

    holding_tickers = {
        ticker_by_instrument.get(str(x))
        for x in history.get("instrument", pd.Series(dtype=str)).dropna()
        if ticker_by_instrument.get(str(x))
    }
    fund_tickers = {
        str(row.get("ticker"))
        for row in latest_market_prices.values()
        if row.get("ticker")
    }
    tickers = sorted({*holding_tickers, *fund_tickers})
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
                "latest_nav": latest_navs.get(code),
                "latest_market_price": latest_market_prices.get(code),
                "etf_performance": performance.get(
                    latest_market_prices.get(code, {}).get("ticker"),
                    {"d1": None, "d7": None, "d30": None},
                ),
                "estimated_nav_gap": estimate_premium_discount_to_nav(latest_navs.get(code), latest_market_prices.get(code)),
                "nav_price_history": nav_price_history_series.get(code, []),
                "holdings": holdings,
                "monthly_changes": monthly_changes_by_fund.get(code, {"previous_month": None, "current_month": None, "changes": []}),
                "monthly_holdings_history": monthly_history_by_fund.get(code, {"months": [], "rows": []}),
            }
        )

    return {"generated_at_utc": utc_now_iso(), "funds": funds}


def main() -> None:
    nav_price_history = derive_nav_price_history(read_csv_if_exists(NAV_HISTORY_PATH), read_csv_if_exists(MARKET_PRICE_HISTORY_PATH))
    write_csv(nav_price_history, NAV_PRICE_HISTORY_PATH)
    SITE_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = build_payload()
    SITE_DATA_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {SITE_DATA_PATH} for {len(payload['funds'])} funds.")


if __name__ == "__main__":
    main()
