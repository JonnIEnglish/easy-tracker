import pandas as pd


def test_cash_can_have_blank_yfinance_ticker() -> None:
    t = pd.read_csv("config/ticker_map.csv")
    cash = t[t["exchange"] == "CASH"]
    assert not cash.empty
    assert cash["yfinance_ticker"].isna().all()
