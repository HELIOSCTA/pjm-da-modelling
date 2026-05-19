"""Loaders for the 7-day load forecast diff report.

Two sources, normalized into the same shape so the fragment layer
treats them identically:

    PJM    -> pjm.seven_day_load_forecast_v1_2025_08_13
              (RTO_COMBINED + zone breakdowns)
    METEO  -> meteologica.usa_pjm_power_demand_forecast_hourly
              (single PJM/RTO-equivalent series)

Both return rows keyed on (source, forecast_area, forecast_date,
evaluated_at_ept, he_start).
"""
from __future__ import annotations

import pandas as pd

from backend.utils.azure_postgresql_utils import pull_from_db


TABLE = "pjm.seven_day_load_forecast_v1_2025_08_13"

# Meteologica stores each PJM zone in its own per-zone table. Keys match
# PJM's forecast_area values so the fragment layer can index both sources
# off the same (source, forecast_area) key.
METEO_TABLES: dict[str, str] = {
    "RTO_COMBINED":        "meteologica.usa_pjm_power_demand_forecast_hourly",
    "MID_ATLANTIC_REGION": "meteologica.usa_pjm_midatlantic_power_demand_forecast_hourly",
    "WESTERN_REGION":      "meteologica.usa_pjm_west_power_demand_forecast_hourly",
    "SOUTHERN_REGION":     "meteologica.usa_pjm_south_power_demand_forecast_hourly",
}


def load_forecast_window(
    *,
    forecast_areas: list[str] | None = None,
    eval_lookback_days: int = 2,
) -> pd.DataFrame:
    """Return forecast rows whose evaluated_at_utc is within the window.

    Window:
        evaluated_at_datetime_utc >= (current_date - eval_lookback_days)::timestamp
        evaluated_at_datetime_utc <  (current_date + 1)::timestamp

    Columns returned:
        evaluated_at_utc, evaluated_at_ept,
        forecast_dt_utc, forecast_dt_ept,
        forecast_area, forecast_load_mw,
        forecast_date (date, EPT-local), he_start (0..23, EPT-local hour).
    """
    area_filter = ""
    if forecast_areas:
        joined = ", ".join(f"'{a}'" for a in forecast_areas)
        area_filter = f"AND forecast_area IN ({joined})"

    query = f"""
        SELECT evaluated_at_datetime_utc,
               evaluated_at_datetime_ept,
               forecast_datetime_beginning_utc,
               forecast_datetime_beginning_ept,
               forecast_area,
               forecast_load_mw
        FROM {TABLE}
        WHERE evaluated_at_datetime_utc >= (current_date - {eval_lookback_days})::timestamp
          AND evaluated_at_datetime_utc <  (current_date + 1)::timestamp
          {area_filter}
    """

    df = pull_from_db(query=query)
    if df is None:
        raise RuntimeError(
            f"pull_from_db returned None for {TABLE} — check Azure Postgres connectivity."
        )
    if df.empty:
        return df

    df = df.rename(columns={
        "evaluated_at_datetime_utc": "evaluated_at_utc",
        "evaluated_at_datetime_ept": "evaluated_at_ept",
        "forecast_datetime_beginning_utc": "forecast_dt_utc",
        "forecast_datetime_beginning_ept": "forecast_dt_ept",
    })

    for col in ("evaluated_at_utc", "evaluated_at_ept", "forecast_dt_utc", "forecast_dt_ept"):
        df[col] = pd.to_datetime(df[col])

    df["forecast_load_mw"] = pd.to_numeric(df["forecast_load_mw"], errors="coerce")
    df["forecast_date"] = df["forecast_dt_ept"].dt.date
    df["he_start"] = df["forecast_dt_ept"].dt.hour.astype(int)
    df["source"] = "PJM"

    return df.sort_values(
        ["source", "forecast_area", "forecast_date", "he_start", "evaluated_at_utc"]
    ).reset_index(drop=True)


def load_meteologica_window(
    *,
    forecast_areas: list[str] | None = None,
    eval_lookback_days: int = 2,
) -> pd.DataFrame:
    """Return Meteologica rows for each requested PJM area, unioned.

    Meteologica stores `issue_date` as a varchar like
    "2026-05-15 13:19:44 UTC" and `forecast_period_start` as a naive
    timestamp in local EPT (utc_offset_from = UTC-04/-05). One table per
    PJM zone — we read each requested zone's table and stamp the
    matching `forecast_area` so the frame merges cleanly with PJM.
    """
    areas = list(forecast_areas) if forecast_areas else list(METEO_TABLES.keys())
    frames: list[pd.DataFrame] = []
    for area in areas:
        table = METEO_TABLES.get(area)
        if table is None:
            continue
        frame = _read_meteo_table(table=table, eval_lookback_days=eval_lookback_days)
        if frame is None or frame.empty:
            continue
        frame["forecast_area"] = area
        frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=[
            "source", "forecast_area",
            "evaluated_at_utc", "evaluated_at_ept",
            "forecast_dt_utc", "forecast_dt_ept",
            "forecast_load_mw", "forecast_date", "he_start",
        ])

    df = pd.concat(frames, ignore_index=True, sort=False)
    df["source"] = "METEO"

    keep = [
        "source", "forecast_area",
        "evaluated_at_utc", "evaluated_at_ept",
        "forecast_dt_utc", "forecast_dt_ept",
        "forecast_load_mw", "forecast_date", "he_start",
    ]
    return df[keep].sort_values(
        ["source", "forecast_area", "forecast_date", "he_start", "evaluated_at_utc"]
    ).reset_index(drop=True)


def _read_meteo_table(*, table: str, eval_lookback_days: int) -> pd.DataFrame | None:
    """Read one Meteologica zone table and normalize the timestamp columns."""
    query = f"""
        SELECT issue_date,
               forecast_period_start,
               forecast_period_end,
               forecast_mw
        FROM {table}
        WHERE created_at >= ((current_date - {eval_lookback_days})::timestamp AT TIME ZONE 'UTC')
          AND created_at <  ((current_date + 1)::timestamp AT TIME ZONE 'UTC')
    """
    df = pull_from_db(query=query)
    if df is None:
        raise RuntimeError(
            f"pull_from_db returned None for {table} — check Azure Postgres connectivity."
        )
    if df.empty:
        return df

    issue_utc = pd.to_datetime(
        df["issue_date"].str.replace(" UTC", "", regex=False),
        utc=True, errors="coerce",
    )
    df["evaluated_at_utc"] = issue_utc.dt.tz_localize(None)
    df["evaluated_at_ept"] = (
        issue_utc.dt.tz_convert("US/Eastern").dt.tz_localize(None)
    )
    df["forecast_dt_ept"] = pd.to_datetime(df["forecast_period_start"])
    df["forecast_dt_utc"] = pd.NaT
    df = df.rename(columns={"forecast_mw": "forecast_load_mw"})
    df["forecast_load_mw"] = pd.to_numeric(df["forecast_load_mw"], errors="coerce")
    df["forecast_date"] = df["forecast_dt_ept"].dt.date
    df["he_start"] = df["forecast_dt_ept"].dt.hour.astype(int)
    return df


def load_combined_window(
    *,
    forecast_areas: list[str] | None = None,
    eval_lookback_days: int = 2,
) -> pd.DataFrame:
    """Union the PJM and Meteologica windows into one normalized frame."""
    pjm = load_forecast_window(
        forecast_areas=forecast_areas,
        eval_lookback_days=eval_lookback_days,
    )
    meteo = load_meteologica_window(
        forecast_areas=forecast_areas,
        eval_lookback_days=eval_lookback_days,
    )
    if pjm.empty and meteo.empty:
        return pjm
    return pd.concat([pjm, meteo], ignore_index=True, sort=False)
