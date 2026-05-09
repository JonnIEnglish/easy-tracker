import pandas as pd
import pytest

from scripts.build_site_data import (
    derive_monthly_holdings_changes,
    derive_monthly_holdings_history,
    estimate_premium_discount_to_nav,
    latest_holdings,
    latest_market_price_by_fund,
    latest_nav_by_fund,
    pct_change,
)


def test_pct_change() -> None:
    assert pct_change(110, 100) == pytest.approx(10)
    assert pct_change(100, 0) is None
    assert pct_change(None, 100) is None


def test_latest_holdings_uses_latest_capture_per_fund() -> None:
    history = pd.DataFrame(
        [
            {"fund_code": "EASYGE", "captured_at_utc": "2026-01-01T00:00:00Z", "instrument": "A"},
            {"fund_code": "EASYGE", "captured_at_utc": "2026-01-02T00:00:00Z", "instrument": "B"},
            {"fund_code": "EASYAI", "captured_at_utc": "2026-01-01T00:00:00Z", "instrument": "C"},
        ]
    )
    latest = latest_holdings(history)
    assert set(latest["instrument"]) == {"B", "C"}


def test_derive_monthly_holdings_changes_added_exited_and_changed() -> None:
    history = pd.DataFrame(
        [
            {"fund_code": "EASYGE", "captured_at_utc": "2026-01-05T10:00:00Z", "instrument": "A", "weight": 10.0},
            {"fund_code": "EASYGE", "captured_at_utc": "2026-01-05T10:00:00Z", "instrument": "B", "weight": 5.0},
            {"fund_code": "EASYGE", "captured_at_utc": "2026-02-02T09:00:00Z", "instrument": "A", "weight": 9.0},
            {"fund_code": "EASYGE", "captured_at_utc": "2026-02-02T09:00:00Z", "instrument": "C", "weight": 6.0},
            {"fund_code": "EASYGE", "captured_at_utc": "2026-02-28T12:00:00Z", "instrument": "A", "weight": 12.0},
            {"fund_code": "EASYGE", "captured_at_utc": "2026-02-28T12:00:00Z", "instrument": "C", "weight": 6.0},
        ]
    )

    derived = derive_monthly_holdings_changes(history)
    fund = derived["EASYGE"]
    assert fund["previous_month"] == "2026-01"
    assert fund["current_month"] == "2026-02"

    changes = {row["instrument"]: row for row in fund["changes"]}
    assert changes["A"]["action"] == "increased"
    assert changes["A"]["previous_weight"] == pytest.approx(10.0)
    assert changes["A"]["current_weight"] == pytest.approx(12.0)
    assert changes["A"]["change_pp"] == pytest.approx(2.0)

    assert changes["B"]["action"] == "exited"
    assert changes["B"]["previous_weight"] == pytest.approx(5.0)
    assert changes["B"]["current_weight"] == pytest.approx(0.0)

    assert changes["C"]["action"] == "added"
    assert changes["C"]["previous_weight"] == pytest.approx(0.0)
    assert changes["C"]["current_weight"] == pytest.approx(6.0)


def test_derive_monthly_holdings_history_uses_latest_snapshot_per_month() -> None:
    history = pd.DataFrame(
        [
            {"fund_code": "EASYGE", "captured_at_utc": "2026-01-05T10:00:00Z", "instrument": "A", "weight": 10.0},
            {"fund_code": "EASYGE", "captured_at_utc": "2026-01-31T10:00:00Z", "instrument": "A", "weight": 12.0},
            {"fund_code": "EASYGE", "captured_at_utc": "2026-01-31T10:00:00Z", "instrument": "B", "weight": 5.0},
            {"fund_code": "EASYGE", "captured_at_utc": "2026-02-28T10:00:00Z", "instrument": "A", "weight": 8.0},
            {"fund_code": "EASYGE", "captured_at_utc": "2026-02-28T10:00:00Z", "instrument": "C", "weight": 7.0},
        ]
    )

    derived = derive_monthly_holdings_history(history)["EASYGE"]
    assert derived["months"] == ["2026-01", "2026-02"]

    rows = {row["instrument"]: row for row in derived["rows"]}
    assert rows["A"]["weights"] == pytest.approx([12.0, 8.0])
    assert rows["B"]["weights"] == pytest.approx([5.0, 0.0])
    assert rows["C"]["weights"] == pytest.approx([0.0, 7.0])
    assert rows["C"]["first_month"] == "2026-02"
    assert rows["B"]["last_month"] == "2026-01"


def test_latest_nav_by_fund_picks_latest_nav_date_then_capture_time() -> None:
    nav_history = pd.DataFrame(
        [
            {
                "fund_code": "EASYGE",
                "nav_zac": 1000.0,
                "nav_date": "2026-05-07",
                "source_url": "u1",
                "captured_at_utc": "2026-05-08T09:00:00Z",
            },
            {
                "fund_code": "EASYGE",
                "nav_zac": 1001.0,
                "nav_date": "2026-05-08",
                "source_url": "u1",
                "captured_at_utc": "2026-05-08T08:00:00Z",
            },
            {
                "fund_code": "EASYGE",
                "nav_zac": 1002.0,
                "nav_date": "2026-05-08",
                "source_url": "u1",
                "captured_at_utc": "2026-05-08T10:00:00Z",
            },
        ]
    )
    latest = latest_nav_by_fund(nav_history)
    assert latest["EASYGE"]["value_zac"] == pytest.approx(1002.0)
    assert latest["EASYGE"]["nav_date"] == "2026-05-08"


def test_latest_market_price_by_fund_picks_latest_price_timestamp() -> None:
    market_history = pd.DataFrame(
        [
            {
                "fund_code": "EASYGE",
                "ticker": "EASYGE.JO",
                "price": 99.0,
                "source": "yfinance",
                "price_at_utc": "2026-05-08T14:00:00Z",
                "captured_at_utc": "2026-05-08T14:05:00Z",
            },
            {
                "fund_code": "EASYGE",
                "ticker": "EASYGE.JO",
                "price": 100.0,
                "source": "yfinance",
                "price_at_utc": "2026-05-08T15:00:00Z",
                "captured_at_utc": "2026-05-08T15:05:00Z",
            },
        ]
    )
    latest = latest_market_price_by_fund(market_history)
    assert latest["EASYGE"]["value_zac"] == pytest.approx(100.0)
    assert latest["EASYGE"]["ticker"] == "EASYGE.JO"


def test_estimate_premium_discount_to_nav_states_and_missing() -> None:
    nav = {"value_zac": 100.0}
    premium = {"value_zac": 102.0}
    discount = {"value_zac": 98.0}
    near_nav = {"value_zac": 100.2}

    premium_result = estimate_premium_discount_to_nav(nav, premium, near_nav_threshold_pct=0.25)
    discount_result = estimate_premium_discount_to_nav(nav, discount, near_nav_threshold_pct=0.25)
    near_result = estimate_premium_discount_to_nav(nav, near_nav, near_nav_threshold_pct=0.25)
    missing_result = estimate_premium_discount_to_nav(nav, None, near_nav_threshold_pct=0.25)

    assert premium_result["status"] == "premium"
    assert premium_result["difference_pct"] == pytest.approx(2.0)
    assert discount_result["status"] == "discount"
    assert discount_result["difference_pct"] == pytest.approx(-2.0)
    assert near_result["status"] == "near_nav"
    assert missing_result["status"] == "n/a"
    assert missing_result["difference_pct"] is None
