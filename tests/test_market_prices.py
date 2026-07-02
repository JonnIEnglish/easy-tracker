import pandas as pd

from scripts.fetch_market_prices import configured_fund_tickers, last_known_nav_zac, parse_price_observation


def test_last_known_nav_zac_picks_latest_capture() -> None:
    nav_history = pd.DataFrame(
        [
            {"fund_code": "EASYGE", "nav_zac": 1000.0, "captured_at_utc": "2026-05-08T10:00:00Z"},
            {"fund_code": "EASYGE", "nav_zac": 1010.0, "captured_at_utc": "2026-05-09T10:00:00Z"},
        ]
    )
    assert last_known_nav_zac(nav_history, "EASYGE") == 1010.0
    assert last_known_nav_zac(nav_history, "EASYBF") is None


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
