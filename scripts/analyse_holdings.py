from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError

from scripts.utils import write_csv

HOLDINGS = Path("data/holdings_history.csv")
THEME_MAP = Path("config/theme_map.csv")
TICKER_MAP = Path("config/ticker_map.csv")
STOCK_PRICES = Path("data/prices/stock_prices.csv")
BENCH_PRICES = Path("data/prices/benchmark_prices.csv")
EASYGE_PRICES = Path("data/prices/easyge_prices.csv")
ANALYTICS_DIR = Path("data/analytics")
METRICS_JSON = Path("site/metrics.json")
DATA_QUALITY_MD = Path("reports/data_quality.md")
DATA_QUALITY_JSON = Path("site/data_quality.json")
ACTIVE_THRESHOLD = 0.25


def classify_change(prev: float, cur: float) -> str:
    if prev == 0 and cur > 0:
        return "new_position"
    if prev > 0 and cur == 0:
        return "exited_position"
    if cur > prev:
        return "increased"
    if cur < prev:
        return "decreased"
    return "unchanged"


def pct_return(prices: pd.DataFrame, ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> float | None:
    p = prices[(prices["ticker"] == ticker) & (prices["date"] >= start) & (prices["date"] <= end)].sort_values("date")
    if p.empty or len(p) < 2:
        return None
    first = float(p.iloc[0]["close"])
    last = float(p.iloc[-1]["close"])
    if first <= 0:
        return None
    return (last / first) - 1


def forward_return(prices: pd.DataFrame, ticker: str, start: pd.Timestamp, months: int) -> float | None:
    end = start + pd.DateOffset(months=months)
    return pct_return(prices, ticker, start, end)


def build_concentration(h: pd.DataFrame) -> pd.DataFrame:
    out = []
    for snapshot_dt, g in h.groupby("snapshot_date"):
        g2 = g.sort_values("weight", ascending=False)
        hhi = float(np.sum((g2["weight"] / 100.0) ** 2))
        out.append(
            {
                "snapshot_date": snapshot_dt,
                "num_holdings": int(len(g2)),
                "cash_weight": float(g2[g2["instrument"].str.contains("CASH", case=False, na=False)]["weight"].sum()),
                "top_1_weight": float(g2.head(1)["weight"].sum()),
                "top_3_weight": float(g2.head(3)["weight"].sum()),
                "top_5_weight": float(g2.head(5)["weight"].sum()),
                "top_10_weight": float(g2.head(10)["weight"].sum()),
                "top_20_weight": float(g2.head(20)["weight"].sum()),
                "total_weight": float(g2["weight"].sum()),
                "hhi": hhi,
                "effective_num_holdings": (1 / hhi) if hhi > 0 else np.nan,
            }
        )
    return pd.DataFrame(out).sort_values("snapshot_date")


def main() -> None:
    if not HOLDINGS.exists():
        raise FileNotFoundError("Missing data/holdings_history.csv")

    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

    h = pd.read_csv(HOLDINGS)
    h["snapshot_date"] = pd.to_datetime(h["snapshot_date"])
    h["weight"] = pd.to_numeric(h["weight"], errors="coerce")
    h = h.dropna(subset=["instrument", "weight"])

    concentration = build_concentration(h)
    write_csv(concentration, ANALYTICS_DIR / "concentration_history.csv")

    latest_date = h["snapshot_date"].max()
    latest = h[h["snapshot_date"] == latest_date].sort_values("weight", ascending=False)
    latest_summary = pd.DataFrame(
        [
            {
                "latest_snapshot_date": latest_date.date().isoformat(),
                "num_holdings": len(latest),
                "cash_weight": float(latest[latest["instrument"].str.contains("CASH", case=False, na=False)]["weight"].sum()),
                "equity_weight": float(latest[~latest["instrument"].str.contains("CASH", case=False, na=False)]["weight"].sum()),
                "top_1_weight": float(latest.head(1)["weight"].sum()),
                "top_3_weight": float(latest.head(3)["weight"].sum()),
                "top_5_weight": float(latest.head(5)["weight"].sum()),
                "top_10_weight": float(latest.head(10)["weight"].sum()),
                "top_20_weight": float(latest.head(20)["weight"].sum()),
                "largest_holding": latest.iloc[0]["instrument"] if not latest.empty else "",
                "largest_holding_weight": float(latest.iloc[0]["weight"] if not latest.empty else 0),
                "total_weight": float(latest["weight"].sum()),
            }
        ]
    )
    write_csv(latest_summary, ANALYTICS_DIR / "latest_summary.csv")

    ticker_map = pd.read_csv(TICKER_MAP) if TICKER_MAP.exists() else pd.DataFrame(columns=["instrument", "ticker", "yfinance_ticker"])
    h_w_ticker = h.merge(ticker_map[["instrument", "ticker", "yfinance_ticker"]], on="instrument", how="left")

    try:
        stock_prices = pd.read_csv(STOCK_PRICES) if STOCK_PRICES.exists() else pd.DataFrame(columns=["date", "ticker", "close"])
    except EmptyDataError:
        stock_prices = pd.DataFrame(columns=["date", "ticker", "close"])
    if not stock_prices.empty:
        stock_prices["date"] = pd.to_datetime(stock_prices["date"])
        stock_prices["close"] = pd.to_numeric(stock_prices["close"], errors="coerce")

    try:
        bench_prices = pd.read_csv(BENCH_PRICES) if BENCH_PRICES.exists() else pd.DataFrame(columns=["date", "ticker", "close"])
    except EmptyDataError:
        bench_prices = pd.DataFrame(columns=["date", "ticker", "close"])
    if not bench_prices.empty:
        bench_prices["date"] = pd.to_datetime(bench_prices["date"])
        bench_prices["close"] = pd.to_numeric(bench_prices["close"], errors="coerce")

    try:
        easyge_prices = pd.read_csv(EASYGE_PRICES) if EASYGE_PRICES.exists() else pd.DataFrame(columns=["date", "ticker", "close"])
    except EmptyDataError:
        easyge_prices = pd.DataFrame(columns=["date", "ticker", "close"])
    if not easyge_prices.empty:
        easyge_prices["date"] = pd.to_datetime(easyge_prices["date"])
        easyge_prices["close"] = pd.to_numeric(easyge_prices["close"], errors="coerce")

    snapshots = sorted(h["snapshot_date"].unique())
    changes_out = []
    turnover_out = []
    contrib_out = []
    active_out = []
    est_port_rets = []

    for i in range(1, len(snapshots)):
        prev_dt, cur_dt = pd.Timestamp(snapshots[i - 1]), pd.Timestamp(snapshots[i])
        prev = h_w_ticker[h_w_ticker["snapshot_date"] == prev_dt][["instrument", "weight", "ticker"]].rename(columns={"weight": "previous_weight"})
        cur = h_w_ticker[h_w_ticker["snapshot_date"] == cur_dt][["instrument", "weight", "ticker"]].rename(columns={"weight": "current_weight"})
        merged = prev.merge(cur[["instrument", "current_weight"]], on="instrument", how="outer").fillna(0)
        merged["weight_change"] = merged["current_weight"] - merged["previous_weight"]
        merged["change_type"] = merged.apply(
            lambda r: classify_change(float(r["previous_weight"]), float(r["current_weight"])), axis=1
        )
        merged["snapshot_date"] = cur_dt.date().isoformat()
        merged["previous_snapshot_date"] = prev_dt.date().isoformat()
        changes_out.append(merged)

        turnover = 0.5 * float(np.abs(merged["weight_change"]).sum())
        counts = merged["change_type"].value_counts().to_dict()
        turnover_out.append(
            {
                "snapshot_date": cur_dt.date().isoformat(),
                "previous_snapshot_date": prev_dt.date().isoformat(),
                "turnover_estimate": turnover,
                "new_positions_count": counts.get("new_position", 0),
                "exited_positions_count": counts.get("exited_position", 0),
                "increased_positions_count": counts.get("increased", 0),
                "decreased_positions_count": counts.get("decreased", 0),
            }
        )

        est_ret_parts = []
        for row in prev.itertuples(index=False):
            if not isinstance(row.ticker, str) or not row.ticker:
                continue
            r = pct_return(stock_prices, row.ticker, prev_dt, cur_dt)
            if r is None:
                continue
            contrib = (float(row.previous_weight) / 100.0) * r
            est_ret_parts.append(contrib)
            contrib_out.append(
                {
                    "previous_snapshot_date": prev_dt.date().isoformat(),
                    "current_snapshot_date": cur_dt.date().isoformat(),
                    "instrument": row.instrument,
                    "previous_weight": float(row.previous_weight),
                    "stock_return": r,
                    "estimated_contribution": contrib,
                }
            )

        est_port_ret = float(np.sum(est_ret_parts)) if est_ret_parts else np.nan
        bench_row = {
            "start_date": prev_dt.date().isoformat(),
            "end_date": cur_dt.date().isoformat(),
            "estimated_portfolio_return": est_port_ret,
        }
        for b in ["SPY", "QQQ", "SMH", "VT"]:
            bench_row[f"benchmark_return_{b.lower()}"] = pct_return(bench_prices, b, prev_dt, cur_dt)
        bench_row["easyge_return_if_available"] = pct_return(easyge_prices, "EASYGE", prev_dt, cur_dt)
        est_port_rets.append(bench_row)

        for row in merged.itertuples(index=False):
            prev_w = float(row.previous_weight)
            cur_w = float(row.current_weight)
            raw_chg = cur_w - prev_w
            ticker = prev[prev["instrument"] == row.instrument]["ticker"]
            ticker_val = ticker.iloc[0] if not ticker.empty else None
            r = pct_return(stock_prices, str(ticker_val), prev_dt, cur_dt) if isinstance(ticker_val, str) and ticker_val else None
            if prev_w == 0 and cur_w > 0:
                signal = "new_position"
                exp = 0.0
                active_change = raw_chg
            elif prev_w > 0 and cur_w == 0:
                signal = "exited_position"
                exp = prev_w
                active_change = -prev_w
            elif r is None or pd.isna(est_port_ret):
                exp = prev_w
                active_change = raw_chg
                signal = "unchanged_or_price_drift"
            else:
                exp = prev_w * (1 + r) / (1 + est_port_ret) if (1 + est_port_ret) != 0 else prev_w
                active_change = cur_w - exp
                if active_change >= ACTIVE_THRESHOLD:
                    signal = "added"
                elif active_change <= -ACTIVE_THRESHOLD:
                    signal = "trimmed"
                else:
                    signal = "unchanged_or_price_drift"

            active_out.append(
                {
                    "snapshot_date": cur_dt.date().isoformat(),
                    "instrument": row.instrument,
                    "previous_weight": prev_w,
                    "current_weight": cur_w,
                    "stock_return": r,
                    "expected_weight_after_price_move": exp,
                    "raw_weight_change": raw_chg,
                    "active_weight_change": active_change,
                    "trade_signal": signal,
                }
            )

    monthly_changes = pd.concat(changes_out, ignore_index=True) if changes_out else pd.DataFrame(
        columns=["snapshot_date", "previous_snapshot_date", "instrument", "previous_weight", "current_weight", "weight_change", "change_type"]
    )
    write_csv(monthly_changes[["snapshot_date", "instrument", "previous_weight", "current_weight", "weight_change", "change_type"]], ANALYTICS_DIR / "monthly_changes.csv")

    turnover_df = pd.DataFrame(
        turnover_out,
        columns=[
            "snapshot_date",
            "previous_snapshot_date",
            "turnover_estimate",
            "new_positions_count",
            "exited_positions_count",
            "increased_positions_count",
            "decreased_positions_count",
        ],
    )
    write_csv(turnover_df, ANALYTICS_DIR / "turnover_history.csv")

    theme_map = pd.read_csv(THEME_MAP) if THEME_MAP.exists() else pd.DataFrame(columns=["instrument", "theme", "subtheme"])
    themed = h.merge(theme_map, on="instrument", how="left")
    themed["theme"] = themed["theme"].fillna("Unmapped")
    themed["subtheme"] = themed["subtheme"].fillna("Unmapped")
    theme_exposure = themed.groupby(["snapshot_date", "theme"], as_index=False)["weight"].sum()
    subtheme_exposure = themed.groupby(["snapshot_date", "theme", "subtheme"], as_index=False)["weight"].sum()
    write_csv(theme_exposure, ANALYTICS_DIR / "theme_exposure.csv")
    write_csv(subtheme_exposure, ANALYTICS_DIR / "theme_subtheme_exposure.csv")

    contribution_df = pd.DataFrame(
        contrib_out,
        columns=["previous_snapshot_date", "current_snapshot_date", "instrument", "previous_weight", "stock_return", "estimated_contribution"],
    )
    write_csv(contribution_df, ANALYTICS_DIR / "contribution_history.csv")

    active_df = pd.DataFrame(
        active_out,
        columns=[
            "snapshot_date",
            "instrument",
            "previous_weight",
            "current_weight",
            "stock_return",
            "expected_weight_after_price_move",
            "raw_weight_change",
            "active_weight_change",
            "trade_signal",
        ],
    )
    write_csv(active_df, ANALYTICS_DIR / "active_trade_estimates.csv")

    write_csv(pd.DataFrame(est_port_rets), ANALYTICS_DIR / "estimated_portfolio_returns.csv")

    score_rows = []
    if not active_df.empty and not stock_prices.empty:
        tick_lookup = ticker_map.set_index("instrument")["ticker"].to_dict() if not ticker_map.empty else {}
        for row in active_df.itertuples(index=False):
            if row.trade_signal not in {"added", "trimmed", "new_position", "exited_position"}:
                continue
            t = tick_lookup.get(row.instrument)
            if not isinstance(t, str) or not t:
                continue
            decision_dt = pd.to_datetime(row.snapshot_date)
            f1 = forward_return(stock_prices, t, decision_dt, 1)
            f3 = forward_return(stock_prices, t, decision_dt, 3)
            bm1 = forward_return(bench_prices, "SPY", decision_dt, 1)
            bm3 = forward_return(bench_prices, "SPY", decision_dt, 3)
            if f1 is None:
                continue
            if row.trade_signal in {"added", "new_position"}:
                hit1 = (bm1 is not None and f1 > bm1)
                hit3 = (bm3 is not None and f3 is not None and f3 > bm3)
            else:
                hit1 = (bm1 is not None and f1 < bm1)
                hit3 = (bm3 is not None and f3 is not None and f3 < bm3)
            score_rows.append(
                {
                    "decision_type": row.trade_signal,
                    "forward_1m_return": f1,
                    "forward_3m_return": f3,
                    "hit_1m": float(hit1) if bm1 is not None else np.nan,
                    "hit_3m": float(hit3) if (bm3 is not None and f3 is not None) else np.nan,
                }
            )

    score_df = pd.DataFrame(score_rows)
    if score_df.empty:
        decision_scorecard = pd.DataFrame(
            columns=[
                "decision_type",
                "count",
                "average_forward_1m_return",
                "median_forward_1m_return",
                "hit_rate_1m",
                "average_forward_3m_return",
                "hit_rate_3m",
            ]
        )
    else:
        agg = score_df.groupby("decision_type", as_index=False).agg(
            count=("decision_type", "count"),
            average_forward_1m_return=("forward_1m_return", "mean"),
            median_forward_1m_return=("forward_1m_return", "median"),
            hit_rate_1m=("hit_1m", "mean"),
            average_forward_3m_return=("forward_3m_return", "mean"),
            hit_rate_3m=("hit_3m", "mean"),
        )
        decision_scorecard = agg
    write_csv(decision_scorecard, ANALYTICS_DIR / "decision_scorecard.csv")

    metrics = latest_summary.iloc[0].to_dict()
    METRICS_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(METRICS_JSON, "w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2)

    dq_lines = ["# Data Quality Warnings", ""]
    unmapped_theme = themed[themed["theme"] == "Unmapped"]["instrument"].drop_duplicates().sort_values().tolist()
    if unmapped_theme:
        dq_lines.append("## Missing Theme Mappings")
        dq_lines.extend([f"- {x}" for x in unmapped_theme])
        dq_lines.append("")
    total_latest = float(latest["weight"].sum())
    if abs(total_latest - 100) > 1:
        dq_lines.append(f"- Holdings total weight not near 100 for latest snapshot: {total_latest:.2f}")
    if STOCK_PRICES.exists() and contribution_df.empty and len(snapshots) > 1:
        dq_lines.append("- Missing stock-price history for contribution calculations.")

    DATA_QUALITY_MD.parent.mkdir(parents=True, exist_ok=True)
    DATA_QUALITY_MD.write_text("\n".join(dq_lines) + "\n", encoding="utf-8")

    with open(DATA_QUALITY_JSON, "w", encoding="utf-8") as fh:
        json.dump({"warnings": dq_lines[2:]}, fh, indent=2)

    print(f"Analytics built for {latest_date.date().isoformat()}.")


if __name__ == "__main__":
    main()
