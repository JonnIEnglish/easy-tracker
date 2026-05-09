from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from scripts.utils import load_funds_config, read_csv_if_exists, utc_now_iso, write_csv

NAV_HISTORY_PATH = Path("data/nav_history.csv")


@dataclass(frozen=True)
class Fund:
    code: str
    instrument_page: str


def configured_funds() -> list[Fund]:
    cfg = load_funds_config()
    return [
        Fund(code=str(row["code"]), instrument_page=str(row["instrument_page"]))
        for row in cfg["funds"]
        if row.get("instrument_page")
    ]


def parse_nav_value(html: str) -> float | None:
    match = re.search(
        r"Price\s*/\s*NAV\s*\(ZAC\).*?([0-9][0-9,\s]*(?:\.[0-9]+)?)",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    raw = re.sub(r"[,\s]", "", match.group(1))
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_date_value(raw: str) -> date | None:
    cleaned = raw.strip().replace(",", "")
    for fmt in ("%d %B %Y", "%d %b %Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return pd.to_datetime(cleaned, format=fmt).date()
        except (ValueError, TypeError):
            continue
    try:
        parsed = pd.to_datetime(cleaned, errors="coerce")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.date()


def parse_nav_date(html: str) -> date | None:
    patterns = [
        r"(?:As[\s-]*of|As[\s-]*at|NAV\s*Date|Date)\s*[:\-]?\s*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4})",
        r"(?:As[\s-]*of|As[\s-]*at|NAV\s*Date|Date)\s*[:\-]?\s*([0-9]{4}-[0-9]{2}-[0-9]{2})",
        r"(?:As[\s-]*of|As[\s-]*at|NAV\s*Date|Date)\s*[:\-]?\s*([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if not match:
            continue
        parsed = _parse_date_value(match.group(1))
        if parsed:
            return parsed
    return None


def parse_nav_observation(html: str, fund_code: str, source_url: str, captured_at_utc: str) -> dict[str, Any] | None:
    nav_value = parse_nav_value(html)
    if nav_value is None:
        return None
    nav_date = parse_nav_date(html)
    if nav_date is None:
        nav_date = pd.to_datetime(captured_at_utc).date()
    return {
        "fund_code": fund_code,
        "nav_zac": float(nav_value),
        "nav_date": nav_date.isoformat(),
        "source_url": source_url,
        "captured_at_utc": captured_at_utc,
    }


def fetch_and_parse(fund: Fund, captured_at_utc: str) -> dict[str, Any] | None:
    response = requests.get(fund.instrument_page, timeout=20)
    response.raise_for_status()
    return parse_nav_observation(response.text, fund.code, fund.instrument_page, captured_at_utc)


def main() -> None:
    captured_at_utc = utc_now_iso()
    history = read_csv_if_exists(NAV_HISTORY_PATH)
    rows: list[dict[str, Any]] = []
    for fund in configured_funds():
        try:
            observation = fetch_and_parse(fund, captured_at_utc)
        except Exception as exc:
            print(f"{fund.code}: failed to fetch NAV ({exc})")
            continue
        if observation is None:
            print(f"{fund.code}: NAV not found on page")
            continue
        rows.append(observation)
        print(f"{fund.code}: captured NAV {observation['nav_zac']} ({observation['nav_date']})")

    if not rows:
        print("No NAV observations captured.")
        return

    new_rows = pd.DataFrame(rows)
    if history.empty:
        merged = new_rows
    else:
        merged = pd.concat([history, new_rows], ignore_index=True)
    merged = merged.drop_duplicates(subset=["fund_code", "nav_date", "nav_zac", "source_url"], keep="last")
    merged = merged.sort_values(["fund_code", "nav_date", "captured_at_utc"])
    write_csv(merged, NAV_HISTORY_PATH)
    print(f"Wrote {NAV_HISTORY_PATH} with {len(new_rows)} new row(s).")


if __name__ == "__main__":
    main()
