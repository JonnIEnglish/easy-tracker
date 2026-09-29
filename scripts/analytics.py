"""Pure analytics over the captured CSV history, feeding the dashboard payload.

Everything here takes DataFrames / plain values and returns JSON-friendly
structures; no network or file IO, so it is unit-testable.
"""
from __future__ import annotations

import math
import re
from typing import Any

import numpy as np
import pandas as pd

# Fund-of-funds sleeves: instrument name held by EASYBF -> underlying fund code.
LOOKTHROUGH_FUNDS = {
    "AI ACTIVELY MANAGED ETF": "EASYAI",
    "EGE ACTIVELY MANAGED ETF": "EASYGE",
}
FUND_LIKE = re.compile(r"ETF|SATRIX|ISSUER|BOND PORTFOLIO", re.IGNORECASE)
EVENT_THRESHOLD_PP = 0.5
SPARK_POINTS = 45


def classify_instrument(name: str) -> str:
    if str(name).upper().startswith("INAV"):
        return "cash"
    if FUND_LIKE.search(str(name)):
        return "fund"
    return "equity"


def _num(value: Any) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _r(value: Any, digits: int = 4) -> float | None:
    f = _num(value)
    return None if f is None else round(f, digits)


# --------------------------------------------------------------------------- #
# Time series (NAV / market price)
# --------------------------------------------------------------------------- #
def daily_nav_series(nav_history: pd.DataFrame, fund_code: str) -> pd.Series:
    """One NAV per published nav_date (latest capture wins); weekend repeats dropped."""
    if nav_history.empty:
        return pd.Series(dtype=float)
    df = nav_history[nav_history["fund_code"] == fund_code].copy()
    if df.empty:
        return pd.Series(dtype=float)
    df["nav_date"] = pd.to_datetime(df["nav_date"], errors="coerce")
    df["captured_at_utc"] = pd.to_datetime(df["captured_at_utc"], utc=True, errors="coerce")
    df["nav_zac"] = pd.to_numeric(df["nav_zac"], errors="coerce")
    df = df.dropna(subset=["nav_date", "nav_zac"]).sort_values(["nav_date", "captured_at_utc"])
    s = df.groupby("nav_date")["nav_zac"].last()
    repeat = s.eq(s.shift())
    weekend = s.index.dayofweek >= 5
    return s[~(repeat & weekend)]


def daily_market_series(price_history: pd.DataFrame, fund_code: str, nav: pd.Series) -> pd.Series:
    from scripts.utils import reconcile_zac_scale

    if price_history.empty:
        return pd.Series(dtype=float)
    df = price_history[price_history["fund_code"] == fund_code].copy()
    if df.empty:
        return pd.Series(dtype=float)
    df["price_at_utc"] = pd.to_datetime(df["price_at_utc"], utc=True, errors="coerce")
    df["captured_at_utc"] = pd.to_datetime(df["captured_at_utc"], utc=True, errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price", "price_at_utc"]).sort_values(["price_at_utc", "captured_at_utc"])
    df["date"] = df["price_at_utc"].dt.tz_convert("UTC").dt.tz_localize(None).dt.normalize()
    s = df.groupby("date")["price"].last()
    if not nav.empty:
        ref = nav.median()
        s = s.map(lambda v: reconcile_zac_scale(v, float(ref)))
    return s


def value_on_or_before(series: pd.Series, target: pd.Timestamp) -> float | None:
    sub = series[series.index <= target]
    return None if sub.empty else float(sub.iloc[-1])


def returns_summary(series: pd.Series) -> dict[str, float | None]:
    """% returns over standard windows, measured back from the latest observation."""
    empty = {k: None for k in ("d1", "d7", "d30", "d90", "ytd", "since_start")}
    if len(series) < 2:
        return empty
    last_date, last = series.index[-1], float(series.iloc[-1])

    def chg(prev: float | None) -> float | None:
        return None if not prev else round((last / prev - 1) * 100, 3)

    ytd_base = value_on_or_before(series, pd.Timestamp(year=last_date.year - 1, month=12, day=31))
    return {
        "d1": chg(float(series.iloc[-2])),
        "d7": chg(value_on_or_before(series, last_date - pd.Timedelta(days=7))),
        "d30": chg(value_on_or_before(series, last_date - pd.Timedelta(days=30))),
        "d90": chg(value_on_or_before(series, last_date - pd.Timedelta(days=90))),
        "ytd": chg(ytd_base),
        "since_start": chg(float(series.iloc[0])),
    }


def risk_summary(series: pd.Series) -> dict[str, Any]:
    if len(series) < 5:
        return {}
    rets = series.pct_change().dropna()
    running_max = series.cummax()
    dd = series / running_max - 1
    worst_idx = rets.idxmin()
    best_idx = rets.idxmax()
    trough = dd.idxmin()
    return {
        "vol_ann_pct": _r(rets.std() * math.sqrt(252) * 100, 2),
        "max_drawdown_pct": _r(dd.min() * 100, 2),
        "max_drawdown_date": trough.date().isoformat(),
        "current_drawdown_pct": _r(dd.iloc[-1] * 100, 2),
        "best_day_pct": _r(rets.max() * 100, 2),
        "best_day_date": best_idx.date().isoformat(),
        "worst_day_pct": _r(rets.min() * 100, 2),
        "worst_day_date": worst_idx.date().isoformat(),
        "up_days_pct": _r((rets > 0).mean() * 100, 1),
        "observations": int(len(rets)),
        "high": _r(series.max(), 2),
        "high_date": series.idxmax().date().isoformat(),
        "low": _r(series.min(), 2),
        "low_date": series.idxmin().date().isoformat(),
        "range_position_pct": _r((series.iloc[-1] - series.min()) / (series.max() - series.min()) * 100, 1)
        if series.max() > series.min()
        else None,
    }


def premium_series(nav_price_history: pd.DataFrame, fund_code: str) -> pd.Series:
    """Last premium/discount % observed on each capture date."""
    if nav_price_history.empty:
        return pd.Series(dtype=float)
    df = nav_price_history[nav_price_history["fund_code"] == fund_code].copy()
    df["difference_pct"] = pd.to_numeric(df["difference_pct"], errors="coerce")
    df["t"] = pd.to_datetime(df["captured_hour_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["difference_pct", "t"]).sort_values("t")
    if df.empty:
        return pd.Series(dtype=float)
    df["date"] = df["t"].dt.tz_localize(None).dt.normalize()
    return df.groupby("date")["difference_pct"].last()


def premium_summary(prem: pd.Series) -> dict[str, Any]:
    if prem.empty:
        return {}
    cur = float(prem.iloc[-1])
    sd = float(prem.std()) if len(prem) > 2 else 0.0
    return {
        "current_pct": _r(cur, 3),
        "mean_pct": _r(prem.mean(), 3),
        "median_pct": _r(prem.median(), 3),
        "max_pct": _r(prem.max(), 3),
        "min_pct": _r(prem.min(), 3),
        "pct_days_premium": _r((prem > 0.25).mean() * 100, 1),
        "pct_days_discount": _r((prem < -0.25).mean() * 100, 1),
        "zscore": _r((cur - prem.mean()) / sd, 2) if sd else None,
        "percentile": _r((prem <= cur).mean() * 100, 0),
        "observations": int(len(prem)),
    }


def series_points(series: pd.Series, digits: int = 3) -> list[list[Any]]:
    return [[d.date().isoformat(), round(float(v), digits)] for d, v in series.items() if pd.notna(v)]


# --------------------------------------------------------------------------- #
# Holdings
# --------------------------------------------------------------------------- #
def weight_matrix(history: pd.DataFrame, fund_code: str) -> pd.DataFrame:
    """Snapshot-date x instrument matrix of weights (%), latest capture per snapshot."""
    df = history[history["fund_code"] == fund_code].copy()
    if df.empty:
        return pd.DataFrame()
    df["captured_at_utc"] = pd.to_datetime(df["captured_at_utc"], utc=True, errors="coerce")
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
    df = df.sort_values("captured_at_utc").drop_duplicates(["snapshot_date", "instrument"], keep="last")
    return df.pivot(index="snapshot_date", columns="instrument", values="weight").sort_index().fillna(0.0)


def currency_map(history: pd.DataFrame, fund_code: str) -> dict[str, str]:
    df = history[history["fund_code"] == fund_code]
    return dict(zip(df["instrument"], df["currency"]))


def concentration(weights: pd.Series, kinds: dict[str, str], currencies: dict[str, str]) -> dict[str, Any]:
    w = weights[weights > 0].sort_values(ascending=False)
    invested = w[[k for k in w.index if kinds.get(k) != "cash"]]
    cash = float(w[[k for k in w.index if kinds.get(k) == "cash"]].sum())
    total = float(w.sum())
    norm = invested / invested.sum() if invested.sum() else invested
    hhi = float((norm**2).sum()) if len(norm) else 0.0
    ccy: dict[str, float] = {}
    for inst, val in w.items():
        ccy[currencies.get(inst, "?")] = ccy.get(currencies.get(inst, "?"), 0.0) + float(val)
    kind_mix: dict[str, float] = {}
    for inst, val in w.items():
        kind_mix[kinds.get(inst, "equity")] = kind_mix.get(kinds.get(inst, "equity"), 0.0) + float(val)
    return {
        "positions": int(len(invested)),
        "total_weight": _r(total, 2),
        "cash_pct": _r(cash, 2),
        "top1_pct": _r(invested.head(1).sum(), 2),
        "top5_pct": _r(invested.head(5).sum(), 2),
        "top10_pct": _r(invested.head(10).sum(), 2),
        "hhi": _r(hhi, 4),
        "effective_n": _r(1 / hhi, 1) if hhi else None,
        "currency": {k: _r(v, 2) for k, v in sorted(ccy.items(), key=lambda kv: -kv[1])},
        "kind": {k: _r(v, 2) for k, v in sorted(kind_mix.items(), key=lambda kv: -kv[1])},
    }


def snapshot_stats(matrix: pd.DataFrame, kinds: dict[str, str]) -> list[dict[str, Any]]:
    """Per-snapshot turnover, cash, concentration and position count."""
    rows: list[dict[str, Any]] = []
    cash_cols = [c for c in matrix.columns if kinds.get(c) == "cash"]
    invested_cols = [c for c in matrix.columns if kinds.get(c) != "cash"]
    prev = None
    for date, row in matrix.iterrows():
        inv = row[invested_cols]
        turnover = None if prev is None else float((row - prev).abs().sum() / 2)
        rows.append(
            {
                "date": date.date().isoformat(),
                "turnover_pct": _r(turnover, 2),
                "cash_pct": _r(row[cash_cols].sum(), 2),
                "top10_pct": _r(inv.sort_values(ascending=False).head(10).sum(), 2),
                "positions": int((inv > 0).sum()),
            }
        )
        prev = row
    return rows


def _weight_on_or_before(matrix: pd.DataFrame, target: pd.Timestamp) -> pd.Series | None:
    sub = matrix[matrix.index <= target]
    return None if sub.empty else sub.iloc[-1]


def holdings_table(
    matrix: pd.DataFrame,
    kinds: dict[str, str],
    currencies: dict[str, str],
    tickers: dict[str, str],
    perf: dict[str, dict[str, float | None]],
) -> list[dict[str, Any]]:
    if matrix.empty:
        return []
    latest_date = matrix.index[-1]
    latest = matrix.iloc[-1]
    prev = matrix.iloc[-2] if len(matrix) > 1 else None
    w7 = _weight_on_or_before(matrix, latest_date - pd.Timedelta(days=7))
    w30 = _weight_on_or_before(matrix, latest_date - pd.Timedelta(days=30))
    first = matrix.iloc[0]
    out = []
    for inst in latest[latest > 0].sort_values(ascending=False).index:
        col = matrix[inst]
        held = col[col > 0]
        first_seen = held.index[0]
        # continuous run ending at latest snapshot
        run_start = col.index[-1]
        for d in reversed(col.index):
            if col[d] > 0:
                run_start = d
            else:
                break
        p = perf.get(inst) or {}
        weight = float(latest[inst])
        d30 = p.get("d30")
        out.append(
            {
                "instrument": inst,
                "kind": kinds.get(inst, "equity"),
                "currency": currencies.get(inst),
                "ticker": tickers.get(inst),
                "weight": _r(weight, 2),
                "chg_1": _r(weight - float(prev[inst]), 2) if prev is not None else None,
                "chg_7d": _r(weight - float(w7[inst]), 2) if w7 is not None else None,
                "chg_30d": _r(weight - float(w30[inst]), 2) if w30 is not None else None,
                "chg_start": _r(weight - float(first[inst]), 2),
                "peak_weight": _r(col.max(), 2),
                "first_seen": first_seen.date().isoformat(),
                "held_since": run_start.date().isoformat(),
                "days_held": int((latest_date - run_start).days),
                "since_start": bool(run_start == matrix.index[0]),
                "perf": {k: _r(p.get(k), 2) for k in ("d1", "d7", "d30")} if p else None,
                "contrib_30d": _r(weight * d30 / 100, 3) if d30 is not None else None,
                "spark": [_r(v, 2) for v in col.iloc[-SPARK_POINTS:].tolist()],
            }
        )
    return out


def activity_events(matrix: pd.DataFrame, kinds: dict[str, str], since: pd.Timestamp | None = None) -> list[dict[str, Any]]:
    """Adds / exits / meaningful weight moves between consecutive snapshots."""
    events: list[dict[str, Any]] = []
    for prev_date, date in zip(matrix.index[:-1], matrix.index[1:]):
        if since is not None and date < since:
            continue
        a, b = matrix.loc[prev_date], matrix.loc[date]
        for inst in matrix.columns:
            if kinds.get(inst) == "cash":
                continue
            before, after = float(a[inst]), float(b[inst])
            if before <= 0 < after:
                kind = "added"
            elif after <= 0 < before:
                kind = "exited"
            elif abs(after - before) >= EVENT_THRESHOLD_PP:
                kind = "increased" if after > before else "decreased"
            else:
                continue
            events.append(
                {
                    "date": date.date().isoformat(),
                    "type": kind,
                    "instrument": inst,
                    "from": _r(before, 2),
                    "to": _r(after, 2),
                    "delta": _r(after - before, 2),
                }
            )
    events.sort(key=lambda e: (e["date"], abs(e["delta"] or 0)), reverse=True)
    return events


def weight_history(matrix: pd.DataFrame, kinds: dict[str, str], top: int = 10) -> dict[str, Any]:
    if matrix.empty:
        return {"dates": [], "series": []}
    latest = matrix.iloc[-1]
    names = [n for n in latest.sort_values(ascending=False).index if kinds.get(n) != "cash"][:top]
    return {
        "dates": [d.date().isoformat() for d in matrix.index],
        "series": [{"instrument": n, "weights": [_r(v, 2) for v in matrix[n].tolist()]} for n in names],
    }


# --------------------------------------------------------------------------- #
# Cross-fund views
# --------------------------------------------------------------------------- #
def _key(name: str) -> str:
    return re.sub(r"\s+", " ", str(name)).strip().upper()


def overlap(latest_by_fund: dict[str, pd.Series], kinds: dict[str, str], a: str, b: str) -> dict[str, Any]:
    if a not in latest_by_fund or b not in latest_by_fund:
        return {}
    wa = {_key(k): v for k, v in latest_by_fund[a].items() if v > 0 and kinds.get(k) != "cash"}
    wb = {_key(k): v for k, v in latest_by_fund[b].items() if v > 0 and kinds.get(k) != "cash"}
    names = {_key(k): k for k in latest_by_fund[a].index}
    common = sorted(set(wa) & set(wb), key=lambda k: -(wa[k] + wb[k]))
    return {
        "a": a,
        "b": b,
        "overlap_pct": _r(sum(min(wa[k], wb[k]) for k in common), 2),
        "shared_count": len(common),
        "rows": [{"instrument": names.get(k, k), a: _r(wa[k], 2), b: _r(wb[k], 2)} for k in common],
    }


def look_through(
    latest_by_fund: dict[str, pd.Series],
    kinds: dict[str, str],
    currencies_by_fund: dict[str, dict[str, str]],
    parent: str,
    top: int = 30,
) -> dict[str, Any]:
    """Expand fund-of-funds sleeves into underlying exposure."""
    if parent not in latest_by_fund:
        return {}
    exposure: dict[str, dict[str, Any]] = {}
    sleeves = []
    direct_other = 0.0
    ccy: dict[str, float] = {}
    buckets = {"Offshore equity": 0.0, "SA equity": 0.0, "Funds & bonds (direct)": 0.0, "Cash": 0.0}

    def add(inst: str, weight: float, source: str, kind: str, currency: str | None) -> None:
        k = _key(inst)
        e = exposure.setdefault(k, {"instrument": inst, "kind": kind, "currency": currency, "total": 0.0, "sources": {}})
        e["total"] += weight
        e["sources"][source] = e["sources"].get(source, 0.0) + weight

    for inst, w in latest_by_fund[parent].items():
        if w <= 0:
            continue
        child = LOOKTHROUGH_FUNDS.get(inst) or LOOKTHROUGH_FUNDS.get(inst.upper())
        if child and child in latest_by_fund:
            sleeves.append({"instrument": inst, "fund": child, "weight": _r(w, 2)})
            for cinst, cw in latest_by_fund[child].items():
                if cw <= 0:
                    continue
                eff = w * cw / 100
                kind = kinds.get(cinst, "equity")
                cur = currencies_by_fund.get(child, {}).get(cinst)
                add(cinst, eff, f"via {child}", kind, cur)
                ccy[cur or "?"] = ccy.get(cur or "?", 0.0) + eff
                buckets["Cash" if kind == "cash" else "Offshore equity" if cur != "ZAR" else "SA equity"] += eff
        else:
            kind = kinds.get(inst, "equity")
            cur = currencies_by_fund.get(parent, {}).get(inst)
            add(inst, float(w), "Direct", kind, cur)
            ccy[cur or "?"] = ccy.get(cur or "?", 0.0) + float(w)
            buckets["Cash" if kind == "cash" else "Funds & bonds (direct)" if kind == "fund" else "SA equity" if cur == "ZAR" else "Offshore equity"] += float(w)
            direct_other += float(w)
    rows = sorted((e for e in exposure.values() if e["kind"] != "cash"), key=lambda e: -e["total"])
    return {
        "parent": parent,
        "sleeves": sleeves,
        "buckets": {k: _r(v, 2) for k, v in buckets.items() if v},
        "currency": {k: _r(v, 2) for k, v in sorted(ccy.items(), key=lambda kv: -kv[1])},
        "rows": [
            {
                "instrument": e["instrument"],
                "currency": e["currency"],
                "total": _r(e["total"], 2),
                "sources": {k: _r(v, 2) for k, v in e["sources"].items()},
            }
            for e in rows[:top]
        ],
    }


def rebased(series_by_fund: dict[str, pd.Series]) -> dict[str, list[list[Any]]]:
    """Index each series to 100 at the latest date they all have data on or before."""
    valid = {k: s for k, s in series_by_fund.items() if len(s)}
    if not valid:
        return {}
    start = max(s.index[0] for s in valid.values())
    out = {}
    for k, s in valid.items():
        s = s[s.index >= start]
        if s.empty:
            continue
        out[k] = series_points(s / s.iloc[0] * 100, 2)
    return out


def data_health(snapshot_log: pd.DataFrame, today: pd.Timestamp) -> dict[str, Any]:
    if snapshot_log.empty:
        return {}
    df = snapshot_log.copy()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    per_fund = {}
    for code, rows in df.groupby("fund_code"):
        dates = sorted(set(rows["snapshot_date"]))
        gaps = [
            {"from": a.date().isoformat(), "to": b.date().isoformat(), "days": int((b - a).days)}
            for a, b in zip(dates[:-1], dates[1:])
            if (b - a).days > 4
        ]
        per_fund[str(code)] = {
            "snapshots": len(dates),
            "first": dates[0].date().isoformat(),
            "last": dates[-1].date().isoformat(),
            "days_since_last": int((today.normalize() - dates[-1]).days),
            "gaps": gaps,
            "avg_total_weight": _r(pd.to_numeric(rows["total_weight"], errors="coerce").mean(), 2),
        }
    return per_fund
