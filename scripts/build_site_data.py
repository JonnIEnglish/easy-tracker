from __future__ import annotations

import json
import math
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd

from scripts import analytics as A
from scripts.utils import load_funds_config, read_csv_if_exists, reconcile_zac_scale, write_csv

HISTORY_PATH = Path("data/holdings_history.csv")
TICKER_MAP_PATH = Path("config/ticker_map.csv")
NAV_HISTORY_PATH = Path("data/nav_history.csv")
MARKET_PRICE_HISTORY_PATH = Path("data/market_price_history.csv")
NAV_PRICE_HISTORY_PATH = Path("data/nav_price_history.csv")
SNAPSHOT_LOG_PATH = Path("data/snapshot_log.csv")
SITE_DATA_PATH = Path("site/data.json")


def pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return ((current / previous) - 1) * 100


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat().replace("+00:00", "Z")
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def latest_holdings(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return history
    history = history.copy()
    history["captured_at_utc"] = pd.to_datetime(history["captured_at_utc"], utc=True)
    idx = history.groupby("fund_code")["captured_at_utc"].idxmax()
    latest_capture = history.loc[idx, ["fund_code", "captured_at_utc"]]
    return history.merge(latest_capture, on=["fund_code", "captured_at_utc"], how="inner")


def flatten_yfinance_columns(columns: pd.Index) -> list[str]:
    flattened = []
    for col in columns:
        if isinstance(col, tuple):
            parts = []
            for part in col:
                if pd.isna(part):
                    continue
                value = str(part).strip()
                if value and value.lower() != "nan":
                    parts.append(value)
            flattened.append("_".join(parts))
        else:
            flattened.append(str(col).strip())
    return flattened


def downloaded_price_history(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date", "close"])

    frame = df.reset_index()
    frame.columns = flatten_yfinance_columns(frame.columns)
    date_col = next(
        (
            col
            for col in frame.columns
            if col.lower() in {"date", "datetime"} or col.lower().startswith(("date_", "datetime_"))
        ),
        None,
    )
    if date_col is None and len(frame.columns) > 0:
        parsed_first_col = pd.to_datetime(frame[frame.columns[0]], errors="coerce")
        if parsed_first_col.notna().any():
            date_col = frame.columns[0]

    close_col = next((col for col in frame.columns if col == "Close" or col.startswith("Close_")), None)
    if not date_col or not close_col:
        return pd.DataFrame(columns=["date", "close"])

    out = frame[[date_col, close_col]].rename(columns={date_col: "date", close_col: "close"})
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    return out.dropna(subset=["close"]).sort_values("date")


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
        out = downloaded_price_history(df)
        if not out.empty:
            prices[ticker] = out
    return prices


def close_on_or_before(prices: pd.DataFrame, target: pd.Timestamp) -> float | None:
    if prices.empty or "date" not in prices.columns:
        return None
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

    # A market price should always trade close to its paired NAV. When the two are
    # off by ~100x, it's a ZAC (cents) vs ZAR (rand) unit mix-up rather than a real
    # premium/discount, so rescale the market price back onto the NAV's units.
    combined["market_price_zac"] = combined.apply(
        lambda row: reconcile_zac_scale(row["market_price_zac"], row["nav_zac"])
        if pd.notna(row["market_price_zac"]) and pd.notna(row["nav_zac"])
        else row["market_price_zac"],
        axis=1,
    )
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


def latest_generated_timestamp_from_easyequities(holdings_history: pd.DataFrame, nav_history: pd.DataFrame) -> str | None:
    timestamps: list[pd.Timestamp] = []
    if not holdings_history.empty and "captured_at_utc" in holdings_history.columns:
        captured = pd.to_datetime(holdings_history["captured_at_utc"], utc=True, errors="coerce").dropna()
        if not captured.empty:
            timestamps.append(captured.max())
    if not nav_history.empty and "captured_at_utc" in nav_history.columns:
        captured = pd.to_datetime(nav_history["captured_at_utc"], utc=True, errors="coerce").dropna()
        if not captured.empty:
            timestamps.append(captured.max())
    if not timestamps:
        return None
    return max(timestamps).isoformat().replace("+00:00", "Z")


def previous_performance(path: Path = SITE_DATA_PATH) -> dict[str, dict[str, float | None]]:
    """Per-ticker stock performance from the last published payload (fallback if price fetch fails)."""
    try:
        old = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    out: dict[str, dict[str, float | None]] = {}
    for fund in old.get("funds", []):
        for h in fund.get("holdings", []):
            perf = h.get("perf") or h.get("performance")
            if h.get("ticker") and perf:
                out[h["ticker"]] = {k: perf.get(k) for k in ("d1", "d7", "d30")}
    return out


def build_payload() -> dict[str, Any]:
    cfg = load_funds_config()
    funds_cfg = {row["code"]: row for row in cfg["funds"]}
    history = read_csv_if_exists(HISTORY_PATH)
    nav_history = read_csv_if_exists(NAV_HISTORY_PATH)
    market_history = read_csv_if_exists(MARKET_PRICE_HISTORY_PATH)
    snapshot_log = read_csv_if_exists(SNAPSHOT_LOG_PATH)
    ticker_map = read_csv_if_exists(TICKER_MAP_PATH)
    nav_price = derive_nav_price_history(nav_history, market_history)

    ticker_by_instrument: dict[str, str] = {}
    if not ticker_map.empty:
        active = ticker_map[ticker_map["yfinance_ticker"].notna()].copy()
        active["yfinance_ticker"] = active["yfinance_ticker"].astype(str).str.strip()
        ticker_by_instrument = dict(zip(active["instrument"].astype(str), active[active["yfinance_ticker"] != ""]["yfinance_ticker"]))
        ticker_by_instrument = {k: v for k, v in ticker_by_instrument.items() if isinstance(v, str) and v}

    latest = latest_holdings(history)
    held = {ticker_by_instrument[i] for i in latest.get("instrument", pd.Series(dtype=str)).astype(str) if i in ticker_by_instrument}
    prices = fetch_price_history(sorted(held))
    perf_by_ticker = {**previous_performance(), **{t: performance_for(df) for t, df in prices.items()}}

    kinds: dict[str, str] = {}
    currencies_by_fund: dict[str, dict[str, str]] = {}
    latest_by_fund: dict[str, pd.Series] = {}
    matrices: dict[str, pd.DataFrame] = {}
    for code in funds_cfg:
        m = A.weight_matrix(history, code)
        matrices[code] = m
        currencies_by_fund[code] = A.currency_map(history, code)
        for inst in m.columns:
            kinds[inst] = A.classify_instrument(inst)
        if not m.empty:
            latest_by_fund[code] = m.iloc[-1]

    latest_navs = latest_nav_by_fund(nav_history)
    nav_series: dict[str, pd.Series] = {}
    funds: list[dict[str, Any]] = []
    for code, fcfg in funds_cfg.items():
        matrix = matrices[code]
        nav = A.daily_nav_series(nav_history, code)
        market = A.daily_market_series(market_history, code, nav)
        prem = A.premium_series(nav_price, code)
        nav_series[code] = nav
        perf = {inst: perf_by_ticker.get(t) for inst, t in ticker_by_instrument.items() if t in perf_by_ticker}
        table = A.holdings_table(matrix, kinds, currencies_by_fund[code], ticker_by_instrument, perf)
        covered = [h for h in table if h["contrib_30d"] is not None]
        covered_w = sum(h["weight"] for h in covered)
        latest_market = latest_market_price_by_fund(market_history).get(code)
        latest_nav = latest_navs.get(code)
        if latest_market and latest_nav:
            latest_market["value_zac"] = reconcile_zac_scale(latest_market["value_zac"], latest_nav["value_zac"])
        events = A.activity_events(matrix, kinds, since=matrix.index[-1] - pd.Timedelta(days=90)) if not matrix.empty else []
        funds.append(
            {
                "code": code,
                "slug": fcfg["slug"],
                "name": fcfg["name"],
                "instrument_page": fcfg.get("instrument_page"),
                "market_ticker": fcfg.get("market_ticker"),
                "snapshot_date": matrix.index[-1].date().isoformat() if not matrix.empty else None,
                "snapshots": int(len(matrix)),
                "nav": latest_nav,
                "market": latest_market,
                "gap": estimate_premium_discount_to_nav(latest_nav, latest_market),
                "nav_returns": A.returns_summary(nav),
                "market_returns": A.returns_summary(market),
                "risk": A.risk_summary(nav),
                "premium": A.premium_summary(prem),
                "series": {
                    "nav": A.series_points(nav, 2),
                    "market": A.series_points(market, 2),
                    "premium": A.series_points(prem, 3),
                },
                "concentration": A.concentration(matrix.iloc[-1], kinds, currencies_by_fund[code]) if not matrix.empty else {},
                "snapshot_stats": A.snapshot_stats(matrix, kinds),
                "holdings": table,
                "attribution": {
                    "covered_weight": round(covered_w, 2),
                    "implied_30d_pct": round(sum(h["contrib_30d"] for h in covered), 3) if covered else None,
                    "top_contributors": [
                        {"instrument": h["instrument"], "contrib": h["contrib_30d"]} for h in sorted(covered, key=lambda h: -h["contrib_30d"])[:5]
                    ],
                    "top_detractors": [
                        {"instrument": h["instrument"], "contrib": h["contrib_30d"]} for h in sorted(covered, key=lambda h: h["contrib_30d"])[:5]
                    ],
                },
                "weight_history": A.weight_history(matrix, kinds),
                "events": events[:400],
            }
        )

    today = pd.Timestamp(latest_generated_timestamp_from_easyequities(history, nav_history) or pd.Timestamp.utcnow()).tz_localize(None)
    overview = {
        "rebased_nav": A.rebased(nav_series),
        "overlap": A.overlap(latest_by_fund, kinds, "EASYGE", "EASYAI"),
        "look_through": A.look_through(latest_by_fund, kinds, currencies_by_fund, "EASYBF"),
        "health": A.data_health(snapshot_log, today),
    }
    return {
        "schema": 2,
        "generated_at_utc": latest_generated_timestamp_from_easyequities(history, nav_history),
        "funds": funds,
        "overview": overview,
    }


def main() -> None:
    nav_price_history = derive_nav_price_history(read_csv_if_exists(NAV_HISTORY_PATH), read_csv_if_exists(MARKET_PRICE_HISTORY_PATH))
    write_csv(nav_price_history, NAV_PRICE_HISTORY_PATH)
    SITE_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = json_safe(build_payload())
    SITE_DATA_PATH.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
    print(f"Wrote {SITE_DATA_PATH} for {len(payload['funds'])} funds.")


if __name__ == "__main__":
    main()
