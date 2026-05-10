from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from hashlib import sha256
from io import StringIO
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

from scripts.utils import load_funds_config, read_csv_if_exists, utc_now, utc_now_iso, write_csv

HISTORY_PATH = Path("data/holdings_history.csv")
SNAPSHOT_LOG_PATH = Path("data/snapshot_log.csv")
RAW_ROOT = Path("data/raw")


class HoldingsValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Fund:
    code: str
    slug: str
    name: str
    holdings_url_template: str


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
        return float(raw)
    except ValueError:
        return None


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
        {"instrument", "security", "name", "constituent", "holding", "description", "instrument_name", "security_name"},
    )
    currency_col = _first_match(df.columns, {"currency", "ccy", "trading_currency"})
    weight_col = _first_match(
        df.columns,
        {"weight", "weight_%", "weighting", "percentage", "portfolio_weight", "holding_weight"},
    )

    if not instrument_col or not currency_col or not weight_col:
        raise HoldingsValidationError(
            f"CSV format changed. columns={df.columns.tolist()}, instrument={instrument_col}, "
            f"currency={currency_col}, weight={weight_col}"
        )

    out = df[[instrument_col, currency_col, weight_col]].copy()
    out.columns = ["instrument", "currency", "weight"]
    out["instrument"] = out["instrument"].astype(str).str.strip()
    out["currency"] = out["currency"].astype(str).str.strip()
    out["weight"] = out["weight"].map(parse_weight)
    out["snapshot_date"] = snapshot_dt.isoformat()
    out = out.dropna(subset=["instrument", "currency", "weight"])
    out = out[out["instrument"] != ""]
    out = out.drop_duplicates(subset=["snapshot_date", "instrument", "currency"], keep="last")
    out["weight"] = out["weight"] * _pick_weight_scale(out["weight"])
    out = out[["snapshot_date", "instrument", "currency", "weight"]]

    if len(out) < 5:
        raise HoldingsValidationError("Parsed fewer than 5 holdings")
    total_weight = float(out["weight"].sum())
    if not (95 <= total_weight <= 105):
        raise HoldingsValidationError(f"Total weight out of expected range: {total_weight:.2f}")

    return out


def configured_funds() -> list[Fund]:
    cfg = load_funds_config()
    return [
        Fund(
            code=str(row["code"]),
            slug=str(row["slug"]),
            name=str(row["name"]),
            holdings_url_template=str(row["holdings_url_template"]),
        )
        for row in cfg["funds"]
    ]


def fetch_latest_snapshot(fund: Fund, max_lookback_days: int) -> tuple[date, str, str]:
    for i in range(max_lookback_days + 1):
        dt = date.today() - timedelta(days=i)
        yyyymmdd = dt.strftime("%Y%m%d")
        url = fund.holdings_url_template.format(yyyymmdd=yyyymmdd)
        try:
            resp = requests.get(url, timeout=20)
        except requests.RequestException:
            continue
        if resp.status_code == 200 and resp.text.strip():
            return dt, url, resp.text

    raise RuntimeError(f"No valid holdings file found for {fund.code} within lookback window")


def raw_path_for(fund: Fund, snapshot_dt: date, csv_text: str, captured_at: str) -> tuple[Path, str]:
    raw_dir = RAW_ROOT / fund.slug
    raw_dir.mkdir(parents=True, exist_ok=True)
    canonical = raw_dir / f"{fund.code}_PCF_ZAR_{snapshot_dt:%Y%m%d}.csv"
    digest = sha256(csv_text.encode("utf-8")).hexdigest()

    if not canonical.exists():
        canonical.write_text(csv_text, encoding="utf-8")
        return canonical, digest

    existing_digest = sha256(canonical.read_text(encoding="utf-8").encode("utf-8")).hexdigest()
    if existing_digest == digest:
        return canonical, digest

    suffix = captured_at.replace("-", "").replace(":", "").replace("T", "_").replace("Z", "Z")
    revision = raw_dir / f"{fund.code}_PCF_ZAR_{snapshot_dt:%Y%m%d}_{suffix}.csv"
    revision.write_text(csv_text, encoding="utf-8")
    return revision, digest


def process_fund(fund: Fund, max_lookback_days: int) -> dict[str, object]:
    captured_at = utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")
    snapshot_dt, source_url, csv_text = fetch_latest_snapshot(fund, max_lookback_days)
    parsed = parse_holdings_csv(csv_text, snapshot_dt)
    raw_file, raw_sha = raw_path_for(fund, snapshot_dt, csv_text, captured_at)

    history = read_csv_if_exists(HISTORY_PATH)
    is_duplicate = False
    if not history.empty and "raw_sha256" in history.columns:
        same_raw = history[
            (history["fund_code"].astype(str) == fund.code)
            & (history["snapshot_date"].astype(str) == snapshot_dt.isoformat())
            & (history["raw_sha256"].astype(str) == raw_sha)
        ]
        is_duplicate = not same_raw.empty

    if not is_duplicate:
        parsed.insert(0, "fund", fund.slug)
        parsed.insert(1, "fund_code", fund.code)
        parsed.insert(2, "fund_name", fund.name)
        parsed["source_url"] = source_url
        parsed["raw_file"] = str(raw_file)
        parsed["raw_sha256"] = raw_sha
        parsed["captured_at_utc"] = captured_at
        history = pd.concat([history, parsed], ignore_index=True) if not history.empty else parsed
        history = history.sort_values(["fund_code", "snapshot_date", "captured_at_utc", "weight"], ascending=[True, True, True, False])
        write_csv(history, HISTORY_PATH)

    status = "duplicate" if is_duplicate else "new"
    if not is_duplicate:
        snapshot_log = read_csv_if_exists(SNAPSHOT_LOG_PATH)
        log_row = pd.DataFrame(
            [
                {
                    "fund": fund.slug,
                    "fund_code": fund.code,
                    "snapshot_date": snapshot_dt.isoformat(),
                    "source_url": source_url,
                    "raw_file": str(raw_file),
                    "raw_sha256": raw_sha,
                    "status": status,
                    "num_holdings": int(len(parsed)),
                    "total_weight": round(float(parsed["weight"].sum()), 4),
                    "captured_at_utc": captured_at,
                }
            ]
        )
        snapshot_log = pd.concat([snapshot_log, log_row], ignore_index=True) if not snapshot_log.empty else log_row
        snapshot_log = snapshot_log.drop_duplicates(subset=["fund_code", "raw_sha256"], keep="last")
        snapshot_log = snapshot_log.sort_values(["fund_code", "captured_at_utc"])
        write_csv(snapshot_log, SNAPSHOT_LOG_PATH)

    return {
        "fund_code": fund.code,
        "snapshot_date": snapshot_dt.isoformat(),
        "status": status,
        "raw_file": str(raw_file),
        "num_holdings": int(len(parsed)),
    }


def main() -> None:
    cfg = load_funds_config()
    max_lookback_days = int(cfg.get("max_holdings_lookback_days", 21))
    results = [process_fund(fund, max_lookback_days) for fund in configured_funds()]
    for result in results:
        print(
            f"{result['fund_code']}: {result['status']} {result['snapshot_date']} "
            f"({result['num_holdings']} holdings, {result['raw_file']})"
        )
    print(f"Finished at {utc_now_iso()}.")


if __name__ == "__main__":
    main()
