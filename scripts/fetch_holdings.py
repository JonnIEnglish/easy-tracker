from __future__ import annotations

from datetime import date, timedelta
from io import StringIO
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

from scripts.utils import load_fund_config, read_csv_if_exists, utc_now_iso, write_csv

HISTORY_PATH = Path("data/holdings_history.csv")
SNAPSHOT_LOG_PATH = Path("data/snapshot_log.csv")
RAW_DIR = Path("data/raw/holdings")
DATA_QUALITY_PATH = Path("reports/data_quality.md")


class HoldingsValidationError(RuntimeError):
    pass


def _normalize_col(col: str) -> str:
    return col.strip().lower().replace(" ", "_")


def _first_match(columns: Iterable[str], candidates: set[str]) -> str | None:
    for col in columns:
        if col in candidates:
            return col
    return None


def parse_weight(value: object) -> float | None:
    if pd.isna(value):
        return None
    raw = str(value).strip().replace(",", "")
    if not raw:
        return None

    if raw.endswith("%"):
        raw = raw[:-1].strip()

    try:
        num = float(raw)
    except ValueError:
        return None

    return num


def _pick_weight_scale(values: pd.Series) -> float:
    total = float(values.sum())
    if 95 <= total <= 105:
        return 1.0

    candidates = [1.0, 100.0, 0.01]
    best = min(candidates, key=lambda scale: abs((total * scale) - 100.0))
    scaled_total = total * best
    if not (95 <= scaled_total <= 105):
        raise HoldingsValidationError(f"Total weight out of expected range: {scaled_total:.2f}")
    return best


def parse_holdings_csv(csv_text: str, snapshot_dt: date) -> pd.DataFrame:
    df = pd.read_csv(StringIO(csv_text))
    df.columns = [_normalize_col(c) for c in df.columns]

    instrument_col = _first_match(
        df.columns,
        {"instrument", "security", "name", "constituent", "holding", "description"},
    )
    currency_col = _first_match(df.columns, {"currency", "ccy"})
    weight_col = _first_match(df.columns, {"weight", "weight_%", "weighting", "percentage"})

    if not instrument_col or not currency_col or not weight_col:
        raise HoldingsValidationError(
            f"CSV format changed. columns={df.columns.tolist()}, instrument={instrument_col}, currency={currency_col}, weight={weight_col}"
        )

    out = df[[instrument_col, currency_col, weight_col]].copy()
    out.columns = ["instrument", "currency", "weight"]
    out["instrument"] = out["instrument"].astype(str).str.strip()
    out["currency"] = out["currency"].astype(str).str.strip()
    out["weight"] = out["weight"].map(parse_weight)
    out["snapshot_date"] = snapshot_dt.isoformat()
    out = out.dropna(subset=["instrument", "weight"])
    out = out[out["instrument"] != ""]
    out["weight"] = out["weight"] * _pick_weight_scale(out["weight"])

    out = out[["snapshot_date", "instrument", "currency", "weight"]]
    out = out.drop_duplicates(subset=["snapshot_date", "instrument", "currency"], keep="last")

    if len(out) < 5:
        raise HoldingsValidationError("Parsed fewer than 5 holdings")
    total_weight = float(out["weight"].sum())
    if not (95 <= total_weight <= 105):
        raise HoldingsValidationError(f"Total weight out of expected range: {total_weight:.2f}")
    if out["instrument"].isna().any() or out["weight"].isna().any():
        raise HoldingsValidationError("Null instruments or weights detected")

    return out


def fetch_latest_snapshot() -> tuple[date, str, str]:
    cfg = load_fund_config()
    fund_cfg = cfg["fund"]
    template = fund_cfg["holdings_url_template"]
    lookback = int(fund_cfg.get("max_holdings_lookback_days", 14))

    for i in range(lookback + 1):
        dt = date.today() - timedelta(days=i)
        yyyymmdd = dt.strftime("%Y%m%d")
        url = template.format(yyyymmdd=yyyymmdd)
        try:
            resp = requests.get(url, timeout=20)
        except requests.RequestException:
            continue
        if resp.status_code == 200 and resp.text.strip():
            return dt, url, resp.text

    raise RuntimeError("No valid holdings file found within lookback window")


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    snapshot_dt, source_url, csv_text = fetch_latest_snapshot()
    parsed = parse_holdings_csv(csv_text, snapshot_dt)

    raw_file = RAW_DIR / f"EASYGE_PCF_ZAR_{snapshot_dt.strftime('%Y%m%d')}.csv"
    raw_file.write_text(csv_text, encoding="utf-8")

    history = read_csv_if_exists(HISTORY_PATH)
    already_seen = False
    if not history.empty and "snapshot_date" in history.columns:
        already_seen = (history["snapshot_date"].astype(str) == snapshot_dt.isoformat()).any()

    combined = pd.concat([history, parsed], ignore_index=True) if not history.empty else parsed
    combined = combined.drop_duplicates(subset=["snapshot_date", "instrument", "currency"], keep="last")
    combined = combined.sort_values(["snapshot_date", "weight"], ascending=[True, False])
    write_csv(combined, HISTORY_PATH)

    snapshot_log = read_csv_if_exists(SNAPSHOT_LOG_PATH)
    log_row = pd.DataFrame(
        [
            {
                "snapshot_date": snapshot_dt.isoformat(),
                "source_url": source_url,
                "raw_file": str(raw_file),
                "status": "already_seen" if already_seen else "new",
                "num_holdings": int(len(parsed)),
                "total_weight": round(float(parsed["weight"].sum()), 4),
                "created_at_utc": utc_now_iso(),
            }
        ]
    )
    snapshot_log = pd.concat([snapshot_log, log_row], ignore_index=True)
    snapshot_log = snapshot_log.drop_duplicates(subset=["snapshot_date", "source_url"], keep="last")
    snapshot_log = snapshot_log.sort_values("snapshot_date")
    write_csv(snapshot_log, SNAPSHOT_LOG_PATH)

    warnings: list[str] = []
    if not parsed["instrument"].str.contains("CASH", case=False, na=False).any():
        warnings.append("- Warning: cash instrument not detected in latest snapshot.")
    total_weight = float(parsed["weight"].sum())
    if abs(total_weight - 100) > 1:
        warnings.append(f"- Warning: total weight not close to 100 (latest={total_weight:.2f}).")

    if warnings:
        DATA_QUALITY_PATH.parent.mkdir(parents=True, exist_ok=True)
        existing = DATA_QUALITY_PATH.read_text(encoding="utf-8") if DATA_QUALITY_PATH.exists() else "# Data Quality Warnings\n\n"
        DATA_QUALITY_PATH.write_text(existing + "\n".join(warnings) + "\n", encoding="utf-8")

    print(f"Processed snapshot {snapshot_dt.isoformat()} ({len(parsed)} holdings).")


if __name__ == "__main__":
    main()
