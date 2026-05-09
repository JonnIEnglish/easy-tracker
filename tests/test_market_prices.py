from scripts.fetch_market_prices import configured_fund_tickers, parse_price_observation


def test_configured_fund_tickers_maps_expected_funds() -> None:
    mapped = {row.fund_code: row.ticker for row in configured_fund_tickers()}
    assert mapped["EASYGE"] == "EASYGE.JO"
    assert mapped["EASYAI"] == "EASYAI.JO"
    assert mapped["EASYBF"] == "EASYBF.JO"


def test_parse_price_observation_handles_missing_and_valid_prices() -> None:
    captured_at_utc = "2026-05-09T10:00:00Z"
    missing = parse_price_observation("EASYGE", "EASYGE.JO", None, None, captured_at_utc)
    invalid = parse_price_observation("EASYGE", "EASYGE.JO", 0.0, None, captured_at_utc)
    valid = parse_price_observation("EASYGE", "EASYGE.JO", 123.45, "2026-05-09T09:30:00Z", captured_at_utc)

    assert missing is None
    assert invalid is None
    assert valid is not None
    assert valid["price"] == 123.45
    assert valid["fund_code"] == "EASYGE"
