import pandas as pd

from scripts.analyse_holdings import classify_change


def test_change_classifier() -> None:
    assert classify_change(0, 1) == "new_position"
    assert classify_change(1, 0) == "exited_position"
    assert classify_change(1, 2) == "increased"
    assert classify_change(2, 1) == "decreased"
    assert classify_change(2, 2) == "unchanged"


def test_turnover_formula_example() -> None:
    df = pd.DataFrame(
        {
            "instrument": ["A", "B", "C"],
            "previous_weight": [50.0, 50.0, 0.0],
            "current_weight": [60.0, 0.0, 40.0],
        }
    )
    df["weight_change"] = df["current_weight"] - df["previous_weight"]
    turnover = 0.5 * df["weight_change"].abs().sum()
    assert turnover == 50.0
