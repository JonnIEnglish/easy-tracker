from datetime import date

import pytest

from scripts.fetch_holdings import parse_holdings_csv, parse_weight


def test_parse_weight_variants() -> None:
    assert parse_weight("9.74") == 9.74
    assert parse_weight("9.74%") == 9.74
    assert parse_weight("0.0974") == 0.0974


def test_parse_holdings_csv_columns_and_dedup() -> None:
    csv_text = """Security,CCY,Weighting
A,USD,50
B,USD,50
A,USD,50
C,USD,0
D,USD,0
E,USD,0
"""
    df = parse_holdings_csv(csv_text, date(2026, 1, 1))
    assert set(df.columns) == {"snapshot_date", "instrument", "currency", "weight"}
    assert len(df[df["instrument"] == "A"]) == 1


def test_parse_holdings_total_weight_validation() -> None:
    bad = """instrument,currency,weight
A,USD,10
B,USD,10
C,USD,10
D,USD,10
E,USD,10
"""
    with pytest.raises(Exception, match="Total weight"):
        parse_holdings_csv(bad, date(2026, 1, 1))
