"""
Shared helpers for the Meteologica PJM historical-forecast archive.

Pulls vintage-stamped forecast issues from the Meteologica xTraders
`historical_data` endpoint, persists raw JSONs to disk, and stacks them into a
long-table parquet keyed on (content_id, issue_date_utc, forecast_period_start_utc).

The same long-table parquet is the source of the derived DA-cutoff parquet
(see derive_da_cutoff.py), which is the drop-in replacement for the existing
rolling cache at modelling/data/cache/meteologica_pjm_*_da_cutoff.parquet.

API contract:
    GET /contents/{content_id}/historical_data/{year}/{month}
        Auth:    ISO account
        Returns: ZIP body. Each entry is a JSON forecast issue.
        404 = no data for that (content, month). Anything else = raise.

Each JSON payload has shape:
    {
        "content_id":   int,
        "content_name": str,
        "issue_date":   "YYYY-MM-DD HH:MM:SS+00:00"  or  "...UTC",
        "timezone":     "America/New_York",
        "unit":         "MW",
        "data": [
            {
                "From yyyy-mm-dd hh:mm":          "YYYY-MM-DD HH:MM",
                "To yyyy-mm-dd hh:mm":            "YYYY-MM-DD HH:MM",
                "UTC offset from (UTC+/-hhmm)":   "UTC+0000",
                "UTC offset to (UTC+/-hhmm)":     "UTC+0000",
                "forecast":                       float,
            },
            ...
        ],
    }
"""

from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from backend.scrapes.meteologica.auth import make_get_request

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

ARCHIVE_ROOT_DEFAULT = Path("modelling/data/cache_archive/meteologica/pjm")
TZ_ET = ZoneInfo("America/New_York")
TZ_UTC = ZoneInfo("UTC")

LONG_COLUMNS = [
    "content_id",
    "content_name",
    "region",
    "variable",
    "issue_date_utc",
    "issue_date_local",
    "forecast_period_start_utc",
    "forecast_period_start_local",
    "forecast_period_end_utc",
    "forecast_horizon_hours",
    "hour_ending",
    "forecast_value_mw",
    "origin_filename",
]

LONG_PRIMARY_KEY = ("content_id", "issue_date_utc", "forecast_period_start_utc")

# 12 PJM contents -- region grain x variable.
# (api_scrape_name, content_id) lifted from backend/scrapes/meteologica/pjm/usa_pjm_*.py.
# value_col matches the column name produced by the existing dbt staging model
# so the derived DA-cutoff parquet can drop into the existing rolling cache slot.
CONTENT_REGISTRY: list[dict] = [
    # RTO
    {"api_scrape_name": "usa_pjm_power_demand_forecast_hourly",          "content_id": 2706, "region": "RTO",    "variable": "load",  "value_col": "forecast_load_mw"},
    {"api_scrape_name": "usa_pjm_pv_power_generation_forecast_hourly",   "content_id": 2553, "region": "RTO",    "variable": "solar", "value_col": "solar_forecast"},
    {"api_scrape_name": "usa_pjm_wind_power_generation_forecast_hourly", "content_id": 2604, "region": "RTO",    "variable": "wind",  "value_col": "wind_forecast"},
    # MIDATL
    {"api_scrape_name": "usa_pjm_midatlantic_power_demand_forecast_hourly",          "content_id": 2688, "region": "MIDATL", "variable": "load",  "value_col": "forecast_load_mw"},
    {"api_scrape_name": "usa_pjm_midatlantic_pv_power_generation_forecast_hourly",   "content_id": 2554, "region": "MIDATL", "variable": "solar", "value_col": "solar_forecast"},
    {"api_scrape_name": "usa_pjm_midatlantic_wind_power_generation_forecast_hourly", "content_id": 2602, "region": "MIDATL", "variable": "wind",  "value_col": "wind_forecast"},
    # SOUTH
    {"api_scrape_name": "usa_pjm_south_power_demand_forecast_hourly",          "content_id": 2722, "region": "SOUTH",  "variable": "load",  "value_col": "forecast_load_mw"},
    {"api_scrape_name": "usa_pjm_south_pv_power_generation_forecast_hourly",   "content_id": 2556, "region": "SOUTH",  "variable": "solar", "value_col": "solar_forecast"},
    {"api_scrape_name": "usa_pjm_south_wind_power_generation_forecast_hourly", "content_id": 2599, "region": "SOUTH",  "variable": "wind",  "value_col": "wind_forecast"},
    # WEST
    {"api_scrape_name": "usa_pjm_west_power_demand_forecast_hourly",          "content_id": 2707, "region": "WEST",   "variable": "load",  "value_col": "forecast_load_mw"},
    {"api_scrape_name": "usa_pjm_west_pv_power_generation_forecast_hourly",   "content_id": 2555, "region": "WEST",   "variable": "solar", "value_col": "solar_forecast"},
    {"api_scrape_name": "usa_pjm_west_wind_power_generation_forecast_hourly", "content_id": 2597, "region": "WEST",   "variable": "wind",  "value_col": "wind_forecast"},
]


# --------------------------------------------------------------------------- #
# Registry lookups
# --------------------------------------------------------------------------- #

def get_registry_entry(api_scrape_name: str) -> dict:
    for entry in CONTENT_REGISTRY:
        if entry["api_scrape_name"] == api_scrape_name:
            return entry
    raise KeyError(f"Unknown api_scrape_name: {api_scrape_name}")


def all_api_scrape_names() -> list[str]:
    return [entry["api_scrape_name"] for entry in CONTENT_REGISTRY]


# --------------------------------------------------------------------------- #
# Timezone parsing
# --------------------------------------------------------------------------- #

def _parse_issue_date(raw: str) -> datetime:
    """Parse the API's `issue_date` string to an aware datetime.

    Accepts ISO-style 'YYYY-MM-DD HH:MM:SS+HH:MM' (or +0000) and the legacy
    ' UTC' suffix shape. Returns a tz-aware datetime; caller can convert.
    """
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        if raw.endswith(" UTC"):
            base = raw[:-4]
            return datetime.fromisoformat(base).replace(tzinfo=timezone.utc)
        raise


_TZ_OFFSET_CACHE: dict[str, timezone] = {}

def _parse_utc_offset(offset_str: str) -> timezone:
    """Parse strings like 'UTC+0000', '-0500', '+0530' into a fixed-offset tz.

    Only the trailing '+HHMM' or '-HHMM' is required; any prefix is stripped.
    Cached because the vast majority of rows share the same offset.
    """
    key = offset_str[-5:]
    if key not in _TZ_OFFSET_CACHE:
        sign = 1 if key[0] == "+" else -1
        hours = int(key[1:3])
        minutes = int(key[3:5])
        _TZ_OFFSET_CACHE[key] = timezone(sign * timedelta(hours=hours, minutes=minutes))
    return _TZ_OFFSET_CACHE[key]


# --------------------------------------------------------------------------- #
# Endpoint pull
# --------------------------------------------------------------------------- #

def pull_historical_month(
    content_id: int, year: int, month: int,
) -> tuple[list[dict], dict[str, dict]]:
    """Fetch one (content_id, year, month) of historical_data.

    Returns:
        payloads:        list of parsed JSON payloads (one per issue).
        raw_by_filename: dict mapping zip entry name -> the raw payload, used
                         by save_raw_payloads() to keep the on-disk filenames
                         identical to the API's natural granule.
    Returns ([], {}) when the API responds 404 (no data for the month).
    """
    endpoint = f"contents/{content_id}/historical_data/{year}/{month}"
    try:
        response = make_get_request(endpoint, account="iso")
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return [], {}
        raise

    payloads: list[dict] = []
    raw_by_filename: dict[str, dict] = {}
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        for entry_name in zf.namelist():
            if not entry_name.endswith(".json"):
                continue
            payload = json.loads(zf.read(entry_name).decode("utf-8"))
            payload["_origin_filename"] = entry_name
            payloads.append(payload)
            raw_by_filename[entry_name] = payload

    return payloads, raw_by_filename


# --------------------------------------------------------------------------- #
# Disk persistence
# --------------------------------------------------------------------------- #

def save_raw_payloads(
    raw_by_filename: dict[str, dict],
    content_id: int,
    year: int,
    month: int,
    archive_root: Path = ARCHIVE_ROOT_DEFAULT,
) -> int:
    """Write each payload's raw JSON under raw/<content_id>/YYYY-MM/<filename>."""
    target_dir = archive_root / "raw" / str(content_id) / f"{year:04d}-{month:02d}"
    target_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    for filename, payload in raw_by_filename.items():
        # Strip our private key before writing to disk.
        clean = {k: v for k, v in payload.items() if not k.startswith("_")}
        out_path = target_dir / Path(filename).name
        out_path.write_text(json.dumps(clean, ensure_ascii=False), encoding="utf-8")
        written += 1
    return written


# --------------------------------------------------------------------------- #
# Long-table construction
# --------------------------------------------------------------------------- #

def payloads_to_long_df(payloads: list[dict], registry_entry: dict) -> pd.DataFrame:
    """Stack a list of issue payloads into a long DataFrame.

    Schema = LONG_COLUMNS. One row per (issue, forecast horizon).
    Skips payloads with empty `data` arrays.
    """
    if not payloads:
        return pd.DataFrame(columns=LONG_COLUMNS)

    rows: list[dict] = []
    for payload in payloads:
        if not payload.get("data"):
            continue

        issue_dt_aware = _parse_issue_date(payload["issue_date"])
        issue_dt_utc = issue_dt_aware.astimezone(TZ_UTC).replace(tzinfo=None)
        issue_dt_local = issue_dt_aware.astimezone(TZ_ET).replace(tzinfo=None)
        origin_filename = payload.get("_origin_filename", "")

        for entry in payload["data"]:
            from_naive = datetime.strptime(entry["From yyyy-mm-dd hh:mm"], "%Y-%m-%d %H:%M")
            to_naive   = datetime.strptime(entry["To yyyy-mm-dd hh:mm"],   "%Y-%m-%d %H:%M")
            tz_from = _parse_utc_offset(entry["UTC offset from (UTC+/-hhmm)"])
            tz_to   = _parse_utc_offset(entry["UTC offset to (UTC+/-hhmm)"])

            from_aware = from_naive.replace(tzinfo=tz_from)
            to_aware   = to_naive.replace(tzinfo=tz_to)

            from_utc = from_aware.astimezone(TZ_UTC).replace(tzinfo=None)
            from_local = from_aware.astimezone(TZ_ET).replace(tzinfo=None)
            to_utc = to_aware.astimezone(TZ_UTC).replace(tzinfo=None)

            horizon_hours = int((from_utc - issue_dt_utc).total_seconds() // 3600)
            hour_ending = from_local.hour + 1  # period-start convention: HE 1 = [00:00, 01:00)

            forecast_raw = entry.get("forecast")
            forecast_val = float(forecast_raw) if forecast_raw is not None else float("nan")

            rows.append({
                "content_id":                  payload["content_id"],
                "content_name":                payload["content_name"],
                "region":                      registry_entry["region"],
                "variable":                    registry_entry["variable"],
                "issue_date_utc":              issue_dt_utc,
                "issue_date_local":            issue_dt_local,
                "forecast_period_start_utc":   from_utc,
                "forecast_period_start_local": from_local,
                "forecast_period_end_utc":     to_utc,
                "forecast_horizon_hours":      horizon_hours,
                "hour_ending":                 hour_ending,
                "forecast_value_mw":           forecast_val,
                "origin_filename":             origin_filename,
            })

    if not rows:
        return pd.DataFrame(columns=LONG_COLUMNS)

    df = pd.DataFrame(rows, columns=LONG_COLUMNS)
    for col in (
        "issue_date_utc", "issue_date_local",
        "forecast_period_start_utc", "forecast_period_start_local",
        "forecast_period_end_utc",
    ):
        df[col] = pd.to_datetime(df[col])
    df["forecast_horizon_hours"] = df["forecast_horizon_hours"].astype("Int64")
    df["hour_ending"] = df["hour_ending"].astype("Int64")
    return df


def long_parquet_path(api_scrape_name: str, archive_root: Path = ARCHIVE_ROOT_DEFAULT) -> Path:
    return archive_root / "long" / f"{api_scrape_name}__vintaged.parquet"


def write_long_parquet(df_new: pd.DataFrame, parquet_path: Path) -> tuple[int, int]:
    """Append df_new to the long parquet, dropping duplicates on PK (keep last).

    Returns (rows_after, rows_added_net).
    """
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    if parquet_path.exists():
        df_existing = pd.read_parquet(parquet_path)
        before = len(df_existing)
        # Concat new last so keep="last" prefers fresh rows on PK collision.
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        before = 0
        df_combined = df_new

    df_combined = df_combined.drop_duplicates(subset=list(LONG_PRIMARY_KEY), keep="last")
    df_combined = df_combined.sort_values(
        ["content_id", "issue_date_utc", "forecast_period_start_utc"],
    ).reset_index(drop=True)

    df_combined = df_combined[LONG_COLUMNS]
    df_combined.to_parquet(parquet_path, index=False)

    after = len(df_combined)
    return after, after - before
