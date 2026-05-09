from datetime import date

from scripts.fetch_holdings import parse_holdings_csv, parse_weight


def test_parse_weight_variants() -> None:
    assert parse_weight("9.74") == 9.74
    assert parse_weight("9.74%") == 9.74
    assert parse_weight("0.0974") == 9.74


def test_parse_holdings_csv_columns_and_dedup() -> None:
    csv_text = """Security,CCY,Weighting\nA,USD,50\nB,USD,50\nA,USD,50\nC,USD,0\nD,USD,0\nE,USD,0\n"""
    df = parse_holdings_csv(csv_text, date(2026, 1, 1))
    assert set(df.columns) == {"snapshot_date", "instrument", "currency", "weight"}
    assert len(df[df["instrument"] == "A"]) == 1


def test_parse_holdings_total_weight_validation() -> None:
    bad = """instrument,currency,weight\nA,USD,10\nB,USD,10\nC,USD,10\nD,USD,10\nE,USD,10\n"""
    try:
        parse_holdings_csv(bad, date(2026, 1, 1))
    except Exception as exc:
        assert "Total weight" in str(exc)
    else:
        raise AssertionError("Expected validation error")
