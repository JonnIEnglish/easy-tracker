import pandas as pd

from scripts import analytics as A


def _history(rows):
    return pd.DataFrame(
        [
            {"fund_code": "F", "snapshot_date": d, "instrument": i, "currency": "USD", "weight": w, "captured_at_utc": d + "T10:00:00Z"}
            for d, i, w in rows
        ]
    )


def test_classify_instrument():
    assert A.classify_instrument("INAV - USD.CASH") == "cash"
    assert A.classify_instrument("SATRIX NAS 100") == "fund"
    assert A.classify_instrument("NVIDIA CORP") == "equity"


def test_returns_and_risk():
    s = pd.Series([100.0, 110.0, 99.0, 120.0], index=pd.to_datetime(["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-10"]))
    r = A.returns_summary(s)
    assert r["d1"] == round((120 / 99 - 1) * 100, 3)
    assert r["since_start"] == 20.0
    k = A.risk_summary(pd.concat([s, pd.Series([118.0], index=pd.to_datetime(["2026-06-11"]))]))
    assert k["max_drawdown_pct"] == -10.0
    assert k["high"] == 120.0


def test_activity_events_and_holdings_table():
    h = _history(
        [("2026-06-01", "A", 10), ("2026-06-01", "B", 5), ("2026-06-02", "A", 11), ("2026-06-02", "C", 4), ("2026-06-03", "A", 11), ("2026-06-03", "C", 4)]
    )
    m = A.weight_matrix(h, "F")
    kinds = {c: "equity" for c in m.columns}
    ev = {(e["date"], e["instrument"]): e["type"] for e in A.activity_events(m, kinds)}
    assert ev[("2026-06-02", "A")] == "increased"
    assert ev[("2026-06-02", "B")] == "exited"
    assert ev[("2026-06-02", "C")] == "added"
    table = A.holdings_table(m, kinds, {}, {}, {})
    assert [t["instrument"] for t in table] == ["A", "C"]
    assert table[1]["held_since"] == "2026-06-02" and not table[1]["since_start"]
    assert table[0]["chg_start"] == 1.0


def test_look_through_expands_sleeves():
    latest = {
        "EASYBF": pd.Series({"AI ACTIVELY MANAGED ETF": 50.0, "CAPITEC LIMITED": 50.0}),
        "EASYAI": pd.Series({"NVIDIA CORP": 60.0, "INAV - USD.CASH": 40.0}),
    }
    kinds = {"AI ACTIVELY MANAGED ETF": "fund", "CAPITEC LIMITED": "equity", "NVIDIA CORP": "equity", "INAV - USD.CASH": "cash"}
    ccy = {"EASYBF": {"AI ACTIVELY MANAGED ETF": "ZAR", "CAPITEC LIMITED": "ZAR"}, "EASYAI": {"NVIDIA CORP": "USD", "INAV - USD.CASH": "USD"}}
    lt = A.look_through(latest, kinds, ccy, "EASYBF")
    rows = {r["instrument"]: r["total"] for r in lt["rows"]}
    assert rows["NVIDIA CORP"] == 30.0 and rows["CAPITEC LIMITED"] == 50.0
    assert lt["buckets"]["Cash"] == 20.0


def test_premium_summary_percentile():
    s = pd.Series([1.0, 2.0, 3.0, -1.0], index=pd.date_range("2026-06-01", periods=4))
    p = A.premium_summary(s)
    assert p["current_pct"] == -1.0 and p["percentile"] == 25.0
