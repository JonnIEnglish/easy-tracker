import json

import pandas as pd
import pytest

from scripts.build_site_data import (
    downloaded_price_history,
    derive_nav_price_history,
    estimate_premium_discount_to_nav,
    json_safe,
    latest_holdings,
    latest_market_price_by_fund,
    latest_nav_by_fund,
    pct_change,
)


def test_pct_change() -> None:
    assert pct_change(110, 100) == pytest.approx(10)
    assert pct_change(100, 0) is None
    assert pct_change(None, 100) is None


def test_json_safe_converts_non_finite_values_to_strict_json_null() -> None:
    payload = {
        "finite": 1.2,
        "missing": float("nan"),
        "nested": [{"positive_infinity": float("inf"), "pandas_missing": pd.NA}],
    }

    text = json.dumps(json_safe(payload), allow_nan=False)

    assert json.loads(text) == {
        "finite": 1.2,
        "missing": None,
        "nested": [{"positive_infinity": None, "pandas_missing": None}],
    }


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


def test_downloaded_price_history_handles_multiindex_date_with_nan_suffix() -> None:
    downloaded = pd.DataFrame(
        {
            ("Close", "EASYGE.JO"): [101.0, 102.5],
            ("Open", "EASYGE.JO"): [100.0, 102.0],
        },
        index=pd.DatetimeIndex(["2026-05-20", "2026-05-21"], name=("Date", float("nan"))),
    )

    history = downloaded_price_history(downloaded)

    assert list(history["date"].astype(str)) == ["2026-05-20", "2026-05-21"]
    assert list(history["close"]) == pytest.approx([101.0, 102.5])


def test_downloaded_price_history_uses_first_datetime_column_when_date_label_is_missing() -> None:
    downloaded = pd.DataFrame(
        {
            "Close": [101.0, None, 103.0],
        },
        index=pd.DatetimeIndex(["2026-05-20", "2026-05-21", "2026-05-22"]),
    )

    history = downloaded_price_history(downloaded)

    assert list(history["date"].astype(str)) == ["2026-05-20", "2026-05-22"]
    assert list(history["close"]) == pytest.approx([101.0, 103.0])


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


def test_derive_nav_price_history_combines_hourly_nav_and_market_price() -> None:
    nav_history = pd.DataFrame(
        [
            {
                "fund_code": "EASYGE",
                "nav_zac": 100.0,
                "nav_date": "2026-05-09",
                "source_url": "u1",
                "captured_at_utc": "2026-05-09T10:05:00Z",
            },
        ]
    )
    market_history = pd.DataFrame(
        [
            {
                "fund_code": "EASYGE",
                "ticker": "EASYGE.JO",
                "price": 102.0,
                "source": "yfinance",
                "price_at_utc": "2026-05-09T10:00:00Z",
                "captured_at_utc": "2026-05-09T10:08:00Z",
            },
        ]
    )

    history = derive_nav_price_history(nav_history, market_history)

    assert len(history) == 1
    row = history.iloc[0]
    assert row["fund_code"] == "EASYGE"
    assert row["captured_hour_utc"] == "2026-05-09T10:00:00Z"
    assert row["nav_zac"] == pytest.approx(100.0)
    assert row["market_price_zac"] == pytest.approx(102.0)
    assert row["difference_zac"] == pytest.approx(2.0)
    assert row["difference_pct"] == pytest.approx(2.0)
    assert row["status"] == "premium"


def test_derive_nav_price_history_corrects_zac_zar_scale_mismatch() -> None:
    nav_history = pd.DataFrame(
        [
            {
                "fund_code": "EASYGE",
                "nav_zac": 11500.0,
                "nav_date": "2026-05-09",
                "source_url": "u1",
                "captured_at_utc": "2026-05-09T10:05:00Z",
            },
        ]
    )
    # Market price reported in rand (115.02) rather than the expected cents (11502.0).
    market_history = pd.DataFrame(
        [
            {
                "fund_code": "EASYGE",
                "ticker": "EASYGE.JO",
                "price": 115.02,
                "source": "yfinance",
                "price_at_utc": "2026-05-09T10:00:00Z",
                "captured_at_utc": "2026-05-09T10:08:00Z",
            },
        ]
    )

    history = derive_nav_price_history(nav_history, market_history)

    row = history.iloc[0]
    assert row["market_price_zac"] == pytest.approx(11502.0)
    assert row["difference_pct"] == pytest.approx(0.017391, rel=1e-3)
    assert row["status"] == "near_nav"
