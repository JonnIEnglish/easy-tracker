import pandas as pd
import pytest

from scripts.build_site_data import latest_holdings, pct_change


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
