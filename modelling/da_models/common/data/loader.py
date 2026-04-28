"""Parquet loaders for shared upstream datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from da_models.common.configs import CACHE_DIR

_DEFAULT_PATTERNS: dict[str, tuple[str, ...]] = {
    "lmps_da": (
        "pjm_lmps_hourly",
        "pjm_lmps_da_hourly",
        "pjm_lmps_da",
        "lmp_da",
        "lmps_da",
        "pjm_lmp_da",
    ),
    "load_rt": ("pjm_load_rt_hourly", "load_rt", "pjm_load_rt", "pjm_load_actual"),
    "load_forecast": (
        "pjm_load_forecast_hourly_da_cutoff",
        "pjm_load_forecast_hourly",
        "load_forecast",
        "pjm_load_forecast",
        "seven_day_load_forecast",
    ),
    "fuel_mix": ("pjm_fuel_mix_hourly", "fuel_mix", "pjm_fuel_mix"),
    "outages_actual": ("pjm_outages_actual_daily", "outages_actual", "pjm_outages_actual"),
    "outages_forecast": (
        "pjm_outages_forecast_daily",
        "outages_forecast",
        "pjm_outages_forecast",
        "seven_day_outage_forecast",
    ),
    "solar_forecast": (
        "pjm_solar_forecast_hourly_da_cutoff",
        "pjm_gridstatus_solar_forecast_hourly",
        "solar_forecast",
        "pjm_solar_forecast",
    ),
    "wind_forecast": (
        "pjm_wind_forecast_hourly_da_cutoff",
        "pjm_gridstatus_wind_forecast_hourly",
        "wind_forecast",
        "pjm_wind_forecast",
    ),
    "weather_observed_hourly": (
        "wsi_pjm_hourly_observed_temp",
        "wsi_weather_observed_hourly",
        "weather_observed_hourly",
        "wsi_pjm_observed_temp_hourly",
    ),
    "weather_forecast_hourly": (
        "wsi_pjm_hourly_forecast_temp_latest",
        "wsi_pjm_hourly_forecast_temp",
        "wsi_weather_forecast_hourly",
        "weather_forecast_hourly",
        "wsi_pjm_forecast_temp_hourly",
    ),
    # Backward-compatible union loader.
    "weather_hourly": (
        "wsi_pjm_hourly_observed_temp",
        "wsi_pjm_hourly_forecast_temp_latest",
        "wsi_pjm_hourly_forecast_temp",
        "wsi_weather_hourly",
        "weather_hourly",
    ),
    "gas_prices_hourly": (
        "ice_python_next_day_gas_hourly",
        "ice_next_day_gas_hourly",
        "ice_gas_prices_hourly",
        "gas_prices_hourly",
        "next_day_gas_hourly",
        "gas_hourly",
    ),
    "meteologica_load_forecast": (
        "meteologica_pjm_load_forecast_hourly_da_cutoff",
        "meteologica_pjm_load_forecast_hourly",
        "meteologica_load_forecast",
    ),
    "meteologica_solar_forecast": (
        "meteologica_pjm_solar_forecast_hourly_da_cutoff",
        "meteologica_pjm_solar_forecast_hourly",
        "meteologica_solar_forecast",
    ),
    "meteologica_wind_forecast": (
        "meteologica_pjm_wind_forecast_hourly_da_cutoff",
        "meteologica_pjm_wind_forecast_hourly",
        "meteologica_wind_forecast",
    ),
    "meteologica_net_load_forecast": (
        "meteologica_pjm_net_load_forecast_hourly_da_cutoff",
        "meteologica_pjm_net_load_forecast_hourly",
        "meteologica_net_load_forecast",
    ),
    # Net-load forecast (RTO-level PJM cutoff first, meteologica regional fallback).
    "net_load_forecast": (
        "pjm_net_load_forecast_hourly_da_cutoff",
        "pjm_net_load_forecast_hourly",
        "meteologica_pjm_net_load_forecast_hourly_da_cutoff",
        "meteologica_pjm_net_load_forecast_hourly",
        "net_load_forecast",
    ),
    "net_load_actual": (
        "pjm_net_load_rt_hourly",
        "pjm_net_load_actual_hourly",
        "net_load_actual",
        "net_load_rt",
    ),
    "day_gen_capacity": (
        "pjm_day_gen_capacity_daily",
        "day_gen_capacity_daily",
        "day_gen_capacity",
    ),
}

_DATE_CANDIDATES = ("date", "forecast_date")
_HOUR_CANDIDATES = ("hour_ending", "hour")
_REGION_CANDIDATES = ("region", "hub", "load_area")


def _resolve_cache_dir(cache_dir: str | Path | None) -> Path:
    if cache_dir is None:
        return CACHE_DIR
    return Path(cache_dir).expanduser()


def _existing_candidates(cache_dir: Path, dataset_key: str) -> list[Path]:
    patterns = _DEFAULT_PATTERNS[dataset_key]
    candidates: list[Path] = []

    for pattern in patterns:
        candidates.extend(cache_dir.glob(f"{pattern}.parquet"))
        candidates.extend(cache_dir.glob(f"{pattern}*.parquet"))

        directory = cache_dir / pattern
        if directory.exists():
            candidates.append(directory)

        candidates.extend(cache_dir.glob(f"{pattern}*"))

    seen: set[Path] = set()
    deduped: list[Path] = []
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(candidate)
    return deduped


def _read_parquet(path: Path, columns: Iterable[str] | None = None) -> pd.DataFrame:
    if path.is_dir():
        return pd.read_parquet(path, columns=list(columns) if columns else None)
    return pd.read_parquet(path, columns=list(columns) if columns else None)


def _first_present(columns: Iterable[str], candidates: tuple[str, ...]) -> str | None:
    column_set = set(columns)
    for candidate in candidates:
        if candidate in column_set:
            return candidate
    return None


def _coerce_date(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_datetime(df[column], errors="coerce").dt.date


def _coerce_hour(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(df[column], errors="coerce").astype("Int64")


def _apply_column_filter(df: pd.DataFrame, columns: Iterable[str] | None) -> pd.DataFrame:
    if columns is None:
        return df
    keep = [column for column in columns if column in df.columns]
    return df[keep].copy()


def _normalize_lmps_da(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    if "market" in output.columns:
        output = output[output["market"].astype(str).str.lower() == "da"].copy()

    date_col = _first_present(output.columns, _DATE_CANDIDATES)
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    region_col = _first_present(output.columns, _REGION_CANDIDATES)
    price_col = _first_present(
        output.columns,
        ("lmp", "lmp_total", "da_lmp_total", "da_lmp"),
    )

    required = {
        "date": date_col,
        "hour_ending": hour_col,
        "region": region_col,
        "lmp": price_col,
    }
    missing = [name for name, column in required.items() if column is None]
    if missing:
        raise KeyError(
            f"Could not normalize lmps_da; missing fields: {missing}. "
            f"Columns: {list(output.columns)}"
        )

    normalized = output[
        [required["date"], required["hour_ending"], required["region"], required["lmp"]]
    ].rename(columns={v: k for k, v in required.items()})
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["lmp"] = pd.to_numeric(normalized["lmp"], errors="coerce")
    normalized["region"] = normalized["region"].astype(str)
    normalized = normalized.dropna(subset=["date", "hour_ending", "lmp"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    return normalized


def _normalize_load_rt(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    date_col = _first_present(output.columns, _DATE_CANDIDATES)
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    region_col = _first_present(output.columns, _REGION_CANDIDATES)
    load_col = _first_present(output.columns, ("rt_load_mw", "load_mw", "load"))

    required = {
        "date": date_col,
        "hour_ending": hour_col,
        "region": region_col,
        "rt_load_mw": load_col,
    }
    missing = [name for name, column in required.items() if column is None]
    if missing:
        raise KeyError(
            f"Could not normalize load_rt; missing fields: {missing}. Columns: {list(output.columns)}"
        )

    normalized = output[
        [required["date"], required["hour_ending"], required["region"], required["rt_load_mw"]]
    ].rename(columns={v: k for k, v in required.items()})
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["rt_load_mw"] = pd.to_numeric(normalized["rt_load_mw"], errors="coerce")
    normalized["region"] = normalized["region"].astype(str)
    normalized = normalized.dropna(subset=["date", "hour_ending", "rt_load_mw"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    return normalized


def _normalize_load_forecast(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    date_col = _first_present(output.columns, ("forecast_date", "date"))
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    region_col = _first_present(output.columns, _REGION_CANDIDATES)
    load_col = _first_present(output.columns, ("forecast_load_mw", "load_forecast"))

    required = {
        "date": date_col,
        "hour_ending": hour_col,
        "region": region_col,
        "forecast_load_mw": load_col,
    }
    missing = [name for name, column in required.items() if column is None]
    if missing:
        raise KeyError(
            f"Could not normalize load_forecast; missing fields: {missing}. "
            f"Columns: {list(output.columns)}"
        )

    normalized = output[
        [
            required["date"],
            required["hour_ending"],
            required["region"],
            required["forecast_load_mw"],
        ]
    ].rename(columns={v: k for k, v in required.items()})
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["forecast_load_mw"] = pd.to_numeric(normalized["forecast_load_mw"], errors="coerce")
    normalized["region"] = normalized["region"].astype(str)
    normalized = normalized.dropna(subset=["date", "hour_ending", "forecast_load_mw"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    return normalized


def _normalize_fuel_mix(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    date_col = _first_present(output.columns, _DATE_CANDIDATES)
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    if date_col is None or hour_col is None:
        raise KeyError(
            "Could not normalize fuel_mix; expected date/hour columns. "
            f"Columns: {list(output.columns)}"
        )

    metadata_columns = {
        "datetime_beginning_utc",
        "datetime_ending_utc",
        "timezone",
        "datetime_beginning_local",
        "datetime_ending_local",
        date_col,
        hour_col,
    }
    numeric_columns = [
        column
        for column in output.columns
        if column not in metadata_columns and pd.api.types.is_numeric_dtype(output[column])
    ]
    normalized = output[[date_col, hour_col, *numeric_columns]].rename(
        columns={date_col: "date", hour_col: "hour_ending"}
    )
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized = normalized.dropna(subset=["date", "hour_ending"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    return normalized


def _normalize_outages_actual(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    date_col = _first_present(output.columns, ("date", "forecast_date", "forecast_execution_date"))
    region_col = _first_present(output.columns, _REGION_CANDIDATES)
    outage_columns = [
        column
        for column in (
            "total_outages_mw",
            "planned_outages_mw",
            "maintenance_outages_mw",
            "forced_outages_mw",
        )
        if column in output.columns
    ]

    if date_col is None or region_col is None or not outage_columns:
        raise KeyError(
            "Could not normalize outages_actual; expected date/region/outage columns. "
            f"Columns: {list(output.columns)}"
        )

    normalized = output[[date_col, region_col, *outage_columns]].rename(
        columns={date_col: "date", region_col: "region"}
    )
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["region"] = normalized["region"].astype(str)
    for column in outage_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized = normalized.dropna(subset=["date"])
    return normalized


def _normalize_outages_forecast(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    date_col = _first_present(output.columns, ("forecast_date", "date"))
    exec_col = _first_present(output.columns, ("forecast_execution_date",))
    region_col = _first_present(output.columns, _REGION_CANDIDATES)
    outage_columns = [
        column
        for column in (
            "total_outages_mw",
            "planned_outages_mw",
            "maintenance_outages_mw",
            "forced_outages_mw",
        )
        if column in output.columns
    ]

    if date_col is None or region_col is None or not outage_columns:
        raise KeyError(
            "Could not normalize outages_forecast; expected forecast_date/region/outage columns. "
            f"Columns: {list(output.columns)}"
        )

    keep = [date_col, region_col, *outage_columns]
    if exec_col is not None:
        keep.append(exec_col)
    if "forecast_day_number" in output.columns:
        keep.append("forecast_day_number")
    if "forecast_rank" in output.columns:
        keep.append("forecast_rank")

    normalized = output[keep].rename(columns={date_col: "date", region_col: "region"})
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["region"] = normalized["region"].astype(str)
    if exec_col is not None:
        normalized = normalized.rename(columns={exec_col: "forecast_execution_date"})
        normalized["forecast_execution_date"] = _coerce_date(normalized, "forecast_execution_date")
        if "forecast_day_number" not in normalized.columns:
            date_ts = pd.to_datetime(normalized["date"], errors="coerce")
            exec_ts = pd.to_datetime(normalized["forecast_execution_date"], errors="coerce")
            normalized["forecast_day_number"] = (date_ts - exec_ts).dt.days + 1
    for column in outage_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    if "forecast_day_number" in normalized.columns:
        normalized["forecast_day_number"] = pd.to_numeric(
            normalized["forecast_day_number"], errors="coerce"
        ).astype("Int64")
    if "forecast_rank" in normalized.columns:
        normalized["forecast_rank"] = pd.to_numeric(normalized["forecast_rank"], errors="coerce").astype(
            "Int64"
        )
    normalized = normalized.dropna(subset=["date"])
    return normalized


def _normalize_solar_forecast(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    date_col = _first_present(output.columns, ("forecast_date", "date"))
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    solar_col = _first_present(output.columns, ("solar_forecast", "forecast_mw"))

    if date_col is None or hour_col is None or solar_col is None:
        raise KeyError(
            "Could not normalize solar_forecast; expected date/hour/solar columns. "
            f"Columns: {list(output.columns)}"
        )

    keep = [date_col, hour_col, solar_col]
    if "solar_forecast_btm" in output.columns:
        keep.append("solar_forecast_btm")
    normalized = output[keep].rename(
        columns={date_col: "date", hour_col: "hour_ending", solar_col: "solar_forecast"}
    )
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["solar_forecast"] = pd.to_numeric(normalized["solar_forecast"], errors="coerce")
    if "solar_forecast_btm" in normalized.columns:
        normalized["solar_forecast_btm"] = pd.to_numeric(
            normalized["solar_forecast_btm"], errors="coerce"
        )
    normalized = normalized.dropna(subset=["date", "hour_ending", "solar_forecast"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    return normalized


def _normalize_wind_forecast(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    date_col = _first_present(output.columns, ("forecast_date", "date"))
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    wind_col = _first_present(output.columns, ("wind_forecast", "forecast_mw"))

    if date_col is None or hour_col is None or wind_col is None:
        raise KeyError(
            "Could not normalize wind_forecast; expected date/hour/wind columns. "
            f"Columns: {list(output.columns)}"
        )

    normalized = output[[date_col, hour_col, wind_col]].rename(
        columns={date_col: "date", hour_col: "hour_ending", wind_col: "wind_forecast"}
    )
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["wind_forecast"] = pd.to_numeric(normalized["wind_forecast"], errors="coerce")
    normalized = normalized.dropna(subset=["date", "hour_ending", "wind_forecast"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    return normalized


def _normalize_weather_hourly(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()

    if "region" in output.columns:
        pjm_rows = output["region"].astype(str).str.upper() == "PJM"
        if pjm_rows.any():
            output = output[pjm_rows].copy()

    date_col = _first_present(output.columns, ("date_ept", "forecast_date_ept", "forecast_date", "date"))
    hour_col = _first_present(output.columns, ("hour_ending_ept", "hour_ending"))
    temp_col = _first_present(output.columns, ("temperature", "temp", "temperature_f", "temp_f"))

    if date_col is None or hour_col is None or temp_col is None:
        raise KeyError(
            "Could not normalize weather_hourly; expected date/hour/temperature columns. "
            f"Columns: {list(output.columns)}"
        )

    optional_map = {
        "feels_like_temp": _first_present(
            output.columns,
            ("feels_like_temperature", "feels_like_temp", "heat_index"),
        ),
        "dew_point_temp": _first_present(output.columns, ("dewpoint", "dew_point_temp", "dew_point")),
        "wind_speed_mph": _first_present(output.columns, ("wind_speed", "wind_speed_mph")),
        "relative_humidity": _first_present(output.columns, ("relative_humidity", "humidity")),
        "cloud_cover_pct": _first_present(output.columns, ("cloud_cover_pct", "cloud_cover")),
    }

    keep = [date_col, hour_col, temp_col]
    for source_col in optional_map.values():
        if source_col is not None:
            keep.append(source_col)
    keep = list(dict.fromkeys(keep))

    rename_map = {
        date_col: "date",
        hour_col: "hour_ending",
        temp_col: "temp",
    }
    for target_col, source_col in optional_map.items():
        if source_col is not None:
            rename_map[source_col] = target_col

    normalized = output[keep].rename(columns=rename_map)
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")

    numeric_cols = [column for column in normalized.columns if column not in ("date", "hour_ending")]
    for column in numeric_cols:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized = normalized.dropna(subset=["date", "hour_ending", "temp"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)

    # WSI has one row per station-hour. Aggregate to PJM date-hour.
    normalized = normalized.groupby(["date", "hour_ending"], as_index=False).mean(numeric_only=True)
    return normalized


def _normalize_gas_prices_hourly(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()

    date_col = _first_present(output.columns, ("date", "gas_day", "forecast_date"))
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    if hour_col is None and "datetime" in output.columns:
        output["hour_ending"] = pd.to_datetime(output["datetime"], errors="coerce").dt.hour + 1
        hour_col = "hour_ending"

    if date_col is None or hour_col is None:
        raise KeyError(
            "Could not normalize gas_prices_hourly; expected date/hour columns. "
            f"Columns: {list(output.columns)}"
        )

    hub_map = {
        "gas_m3": _first_present(output.columns, ("gas_m3", "tetco_m3_cash", "gas_m3_price")),
        "gas_tco": _first_present(output.columns, ("gas_tco", "columbia_tco_cash")),
        "gas_tz6": _first_present(output.columns, ("gas_tz6", "transco_z6_ny_cash")),
        "gas_dom_south": _first_present(
            output.columns,
            ("gas_dom_south", "dominion_south_cash"),
        ),
    }

    keep_hub_columns = [source_col for source_col in hub_map.values() if source_col is not None]
    if not keep_hub_columns:
        raise KeyError(
            "Could not normalize gas_prices_hourly; missing expected hub price columns. "
            f"Columns: {list(output.columns)}"
        )

    keep = [date_col, hour_col, *keep_hub_columns]
    keep = list(dict.fromkeys(keep))

    rename_map = {
        date_col: "date",
        hour_col: "hour_ending",
    }
    for target_col, source_col in hub_map.items():
        if source_col is not None:
            rename_map[source_col] = target_col

    normalized = output[keep].rename(columns=rename_map)
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")

    for column in ("gas_m3", "gas_tco", "gas_tz6", "gas_dom_south"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized = normalized.dropna(subset=["date", "hour_ending"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    normalized = normalized.groupby(["date", "hour_ending"], as_index=False).mean(numeric_only=True)
    return normalized


def _normalize_meteologica_regional(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """Normalizer for Meteologica load/solar/wind parquets.

    Input has: forecast_date, hour_ending, region, <value_col>, forecast_rank,
    forecast_execution_datetime_*. Output keeps the latest forecast per
    (region, forecast_date, hour_ending) — highest forecast_rank.
    """
    output = df.copy()
    date_col = _first_present(output.columns, ("forecast_date", "date"))
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    region_col = _first_present(output.columns, _REGION_CANDIDATES)

    if date_col is None or hour_col is None or region_col is None or value_col not in output.columns:
        raise KeyError(
            f"Could not normalize meteologica frame for {value_col!r}; "
            f"columns: {list(output.columns)}"
        )

    keep = [date_col, hour_col, region_col, value_col]
    if "forecast_rank" in output.columns:
        keep.append("forecast_rank")
    if "forecast_execution_datetime_local" in output.columns:
        keep.append("forecast_execution_datetime_local")

    normalized = output[keep].rename(
        columns={date_col: "date", hour_col: "hour_ending", region_col: "region"}
    )
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["region"] = normalized["region"].astype(str)
    normalized[value_col] = pd.to_numeric(normalized[value_col], errors="coerce")
    normalized = normalized.dropna(subset=["date", "hour_ending", value_col])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)

    # Keep only the highest forecast_rank per (region, date, hour_ending)
    if "forecast_rank" in normalized.columns:
        normalized["forecast_rank"] = pd.to_numeric(
            normalized["forecast_rank"], errors="coerce"
        )
        normalized = normalized.sort_values("forecast_rank", ascending=False)
        normalized = normalized.drop_duplicates(
            subset=["region", "date", "hour_ending"], keep="first"
        )

    return normalized.sort_values(["region", "date", "hour_ending"]).reset_index(drop=True)


def _normalize_meteologica_load(df: pd.DataFrame) -> pd.DataFrame:
    return _normalize_meteologica_regional(df, "forecast_load_mw")


def _normalize_meteologica_solar(df: pd.DataFrame) -> pd.DataFrame:
    return _normalize_meteologica_regional(df, "solar_forecast")


def _normalize_meteologica_wind(df: pd.DataFrame) -> pd.DataFrame:
    return _normalize_meteologica_regional(df, "wind_forecast")


def _normalize_net_load_actual(df: pd.DataFrame) -> pd.DataFrame:
    """PJM RT net-load actuals: hourly load, solar/wind gen, and net_load_mw per region.

    Pre-2019-04-02 hours have NaN solar_gen_mw and therefore NaN net_load_mw, since
    PJM did not publish solar generation actuals before that date. Nulls are preserved
    so consumers can distinguish "PJM didn't report" from "PJM reported zero."
    """
    output = df.copy()
    date_col = _first_present(output.columns, _DATE_CANDIDATES)
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    region_col = _first_present(output.columns, _REGION_CANDIDATES)
    net_load_col = _first_present(output.columns, ("net_load_mw", "net_load"))

    if date_col is None or hour_col is None or region_col is None or net_load_col is None:
        raise KeyError(
            "Could not normalize net_load_actual; expected date/hour/region/net_load_mw. "
            f"Columns: {list(output.columns)}"
        )

    metric_columns = [
        column
        for column in ("rt_load_mw", "solar_gen_mw", "wind_gen_mw", net_load_col)
        if column in output.columns
    ]
    keep = [date_col, hour_col, region_col, *metric_columns]
    keep = list(dict.fromkeys(keep))

    rename_map = {date_col: "date", hour_col: "hour_ending", region_col: "region"}
    if net_load_col != "net_load_mw":
        rename_map[net_load_col] = "net_load_mw"

    normalized = output[keep].rename(columns=rename_map)
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["region"] = normalized["region"].astype(str)
    for column in ("rt_load_mw", "solar_gen_mw", "wind_gen_mw", "net_load_mw"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized = normalized.dropna(subset=["date", "hour_ending"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    return normalized.sort_values(["region", "date", "hour_ending"]).reset_index(drop=True)


def _normalize_meteologica_net_load(df: pd.DataFrame) -> pd.DataFrame:
    """Meteologica net_load combines load/solar/wind/net_load per (region, date, he)."""
    output = df.copy()
    date_col = _first_present(output.columns, ("forecast_date", "date"))
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    region_col = _first_present(output.columns, _REGION_CANDIDATES)
    value_cols = [
        c for c in (
            "forecast_load_mw", "solar_forecast", "wind_forecast", "net_load_forecast_mw",
        ) if c in output.columns
    ]
    if date_col is None or hour_col is None or region_col is None or not value_cols:
        raise KeyError(
            "Could not normalize meteologica_net_load_forecast; "
            f"columns: {list(output.columns)}"
        )
    keep = [date_col, hour_col, region_col, *value_cols]
    normalized = output[keep].rename(
        columns={date_col: "date", hour_col: "hour_ending", region_col: "region"}
    )
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["region"] = normalized["region"].astype(str)
    for col in value_cols:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
    normalized = normalized.dropna(subset=["date", "hour_ending"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    return normalized.sort_values(["region", "date", "hour_ending"]).reset_index(drop=True)


def _normalize_day_gen_capacity(df: pd.DataFrame) -> pd.DataFrame:
    """System-wide daily PJM generation capacity. One row per date, no region/hour."""
    output = df.copy()
    date_col = _first_present(output.columns, _DATE_CANDIDATES)
    capacity_columns = [
        column
        for column in (
            "eco_max_daily_avg_mw",
            "eco_max_daily_min_mw",
            "eco_max_daily_max_mw",
            "emerg_max_daily_avg_mw",
            "total_committed_mw",
        )
        if column in output.columns
    ]

    if date_col is None or not capacity_columns:
        raise KeyError(
            "Could not normalize day_gen_capacity; expected date/capacity columns. "
            f"Columns: {list(output.columns)}"
        )

    normalized = output[[date_col, *capacity_columns]].rename(columns={date_col: "date"})
    normalized["date"] = _coerce_date(normalized, "date")
    for column in capacity_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized = normalized.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return normalized


_NORMALIZERS = {
    "lmps_da": _normalize_lmps_da,
    "load_rt": _normalize_load_rt,
    "load_forecast": _normalize_load_forecast,
    "fuel_mix": _normalize_fuel_mix,
    "outages_actual": _normalize_outages_actual,
    "outages_forecast": _normalize_outages_forecast,
    "solar_forecast": _normalize_solar_forecast,
    "wind_forecast": _normalize_wind_forecast,
    "weather_observed_hourly": _normalize_weather_hourly,
    "weather_forecast_hourly": _normalize_weather_hourly,
    "weather_hourly": _normalize_weather_hourly,
    "gas_prices_hourly": _normalize_gas_prices_hourly,
    "meteologica_load_forecast": _normalize_meteologica_load,
    "meteologica_solar_forecast": _normalize_meteologica_solar,
    "meteologica_wind_forecast": _normalize_meteologica_wind,
    "meteologica_net_load_forecast": _normalize_meteologica_net_load,
    "net_load_forecast": _normalize_meteologica_net_load,
    "net_load_actual": _normalize_net_load_actual,
    "day_gen_capacity": _normalize_day_gen_capacity,
}


def _load_dataset(
    dataset_key: str,
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    if path is not None:
        resolved = Path(path).expanduser()
        if not resolved.exists():
            raise FileNotFoundError(f"Dataset path not found: {resolved}")
        raw = _read_parquet(resolved)
        normalized = _NORMALIZERS[dataset_key](raw)
        return _apply_column_filter(normalized, columns)

    resolved_cache = _resolve_cache_dir(cache_dir)
    candidates = _existing_candidates(resolved_cache, dataset_key)
    if not candidates:
        patterns = ", ".join(_DEFAULT_PATTERNS[dataset_key])
        raise FileNotFoundError(
            f"No parquet data found for '{dataset_key}' in {resolved_cache}. "
            f"Expected names matching: {patterns}."
        )

    read_errors: list[str] = []
    for candidate in candidates:
        try:
            raw = _read_parquet(candidate)
            normalized = _NORMALIZERS[dataset_key](raw)
            return _apply_column_filter(normalized, columns)
        except Exception as exc:  # pragma: no cover - defensive logging path
            read_errors.append(f"{candidate}: {exc}")

    raise RuntimeError(
        f"Found candidates for '{dataset_key}' but could not read any parquet files. "
        f"Errors: {' | '.join(read_errors)}"
    )


def load_lmps_da(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("lmps_da", path=path, cache_dir=cache_dir, columns=columns)


def load_load_rt(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("load_rt", path=path, cache_dir=cache_dir, columns=columns)


def load_load_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("load_forecast", path=path, cache_dir=cache_dir, columns=columns)


def load_fuel_mix(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("fuel_mix", path=path, cache_dir=cache_dir, columns=columns)


def load_outages_actual(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("outages_actual", path=path, cache_dir=cache_dir, columns=columns)


def load_outages_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("outages_forecast", path=path, cache_dir=cache_dir, columns=columns)


def load_solar_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("solar_forecast", path=path, cache_dir=cache_dir, columns=columns)


def load_wind_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("wind_forecast", path=path, cache_dir=cache_dir, columns=columns)


def load_net_load_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("net_load_forecast", path=path, cache_dir=cache_dir, columns=columns)


def load_net_load_actuals(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("net_load_actual", path=path, cache_dir=cache_dir, columns=columns)


def load_day_gen_capacity(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Daily system-wide PJM generation capacity (eco_max stats, total_committed).

    Backward-only feed: today and forward dates won't have rows. For
    forward query use, take the most recently published row — total_committed
    is structural (RPM-cleared, effectively flat day-to-day).
    """
    return _load_dataset("day_gen_capacity", path=path, cache_dir=cache_dir, columns=columns)


def load_weather_hourly(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("weather_hourly", path=path, cache_dir=cache_dir, columns=columns)


def load_weather_observed_hourly(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("weather_observed_hourly", path=path, cache_dir=cache_dir, columns=columns)


def load_weather_forecast_hourly(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("weather_forecast_hourly", path=path, cache_dir=cache_dir, columns=columns)


def load_gas_prices_hourly(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset("gas_prices_hourly", path=path, cache_dir=cache_dir, columns=columns)


def load_meteologica_load_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "meteologica_load_forecast", path=path, cache_dir=cache_dir, columns=columns,
    )


def load_meteologica_solar_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "meteologica_solar_forecast", path=path, cache_dir=cache_dir, columns=columns,
    )


def load_meteologica_wind_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "meteologica_wind_forecast", path=path, cache_dir=cache_dir, columns=columns,
    )


def load_meteologica_net_load_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "meteologica_net_load_forecast", path=path, cache_dir=cache_dir, columns=columns,
    )
