from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_funds_config(path: Path = Path("config/funds.yml")) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def reconcile_zac_scale(value: float, reference: float | None, factor: float = 100.0, tolerance: float = 20.0) -> float:
    """Correct a ZAC (cents) vs ZAR (rand) unit mix-up by comparing against a known-good reference.

    EasyEquities and yfinance sometimes report a value in rand instead of the cents
    convention used everywhere else in this pipeline, which is off by exactly `factor`
    (100) and otherwise looks like a normal number. Comparing against a trusted
    reference for the same instrument (its own prior value, or a paired NAV/price)
    catches that flip: a ratio far outside `tolerance` in either direction means the
    new value is almost certainly in the other unit, so it gets rescaled back to ZAC.
    """
    if reference is None or not (reference > 0) or not (value > 0):
        return value
    ratio = value / reference
    if ratio > tolerance:
        return value / factor
    if ratio < (1 / tolerance):
        return value * factor
    return value
