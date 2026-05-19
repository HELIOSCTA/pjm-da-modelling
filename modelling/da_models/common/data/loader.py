"""Parquet loaders for shared upstream datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from da_models.common.configs import CACHE_DIR

_DEFAULT_PATTERNS: dict[str, tuple[str, ...]] = {
    "lmps_da": ("pjm_lmps_hourly",),
    "lmps_da_sep": ("pjm_lmps_hourly",),
    "lmps_rt": ("pjm_lmps_hourly",),
    "load_rt": ("pjm_load_rt_hourly",),
    "load_forecast": ("pjm_load_forecast_hourly_da_cutoff",),
    "fuel_mix": ("pjm_fuel_mix_hourly",),
    "outages_actual": ("pjm_outages_actual_daily",),
    "outages_forecast": ("pjm_outages_forecast_daily",),
    "outages_forecast_history": ("pjm_outages_forecast_history",),
    "solar_forecast": ("pjm_solar_forecast_hourly_da_cutoff",),
    "wind_forecast": ("pjm_wind_forecast_hourly_da_cutoff",),
    "weather_observed_hourly": ("wsi_pjm_hourly_observed_temp",),
    "weather_forecast_hourly": ("wsi_pjm_hourly_forecast_temp_latest",),
    # Backward-compatible: observed first, latest forecast as fallback.
    "weather_hourly": (
        "wsi_pjm_hourly_observed_temp",
        "wsi_pjm_hourly_forecast_temp_latest",
    ),
    "gas_prices_hourly": ("ice_python_next_day_gas_hourly",),
    "meteologica_load_forecast": ("meteologica_pjm_load_forecast_hourly_da_cutoff",),
    "meteologica_solar_forecast": ("meteologica_pjm_solar_forecast_hourly_da_cutoff",),
    "meteologica_wind_forecast": ("meteologica_pjm_wind_forecast_hourly_da_cutoff",),
    "meteologica_net_load_forecast": (
        "meteologica_pjm_net_load_forecast_hourly_da_cutoff",
    ),
    "meteologica_da_price_forecast": (
        "meteologica_pjm_da_price_forecast_hourly_da_cutoff_historical",
    ),
    # PJM-native net-load forecast only (no Meteologica fallback).
    "pjm_net_load_forecast": ("pjm_net_load_forecast_hourly_da_cutoff",),
    # PJM-native preferred; falls back to Meteologica when missing.
    "net_load_forecast": (
        "pjm_net_load_forecast_hourly_da_cutoff",
        "meteologica_pjm_net_load_forecast_hourly_da_cutoff",
    ),
    "net_load_actual": ("pjm_net_load_rt_hourly",),
    "day_gen_capacity": ("pjm_day_gen_capacity_daily",),
    "installed_capacity": ("ea_pjm_installed_capacity_monthly",),
    "pjm_dates_daily": ("pjm_dates_daily",),
    "reserve_market_results_hourly": ("pjm_reserve_market_results_hourly",),
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


def _apply_column_filter(
    df: pd.DataFrame, columns: Iterable[str] | None
) -> pd.DataFrame:
    if columns is None:
        return df
    keep = [column for column in columns if column in df.columns]
    return df[keep].copy()


def _filter_to_latest_vintage_full_coverage(
    df: pd.DataFrame,
    *,
    region_col: str | None = "region",
    date_col: str = "date",
    hour_col: str = "hour_ending",
    as_of_col: str = "as_of_date",
) -> pd.DataFrame:
    """Two-stage filter that powers the ``latest_only=True`` loader mode.

    Stage 1: pick the single most-recent ``as_of_date``. For region-
    scoped feeds (``region_col`` provided and present in the frame) the
    latest as_of_date is computed PER REGION — different regions can
    publish on different cadences and we don't want one region's lag
    to blank out another. For system-wide feeds (``region_col=None`` or
    absent) a single global max as_of_date is used.

    Stage 2: drop (region, date) tuples — or (date,) tuples for
    system-wide feeds — that don't have all 24 ``hour_ending`` values
    under the chosen vintage. The 24-HE coverage gate matches the
    contract used by the existing lead_days mode.

    Returns an empty DataFrame (preserving columns) when ``df`` is
    empty or lacks an ``as_of_date`` column. Never raises.

    Example::

        # Meteologica multi-vintage forecast frame -> latest publish per
        # region, restricted to forecast_dates with full 24-HE coverage.
        fcst = load_meteologica_net_load_forecast(cache_dir=...)
        latest = _filter_to_latest_vintage_full_coverage(fcst, region_col="region")
        # latest spans D+1 ... D+N for whatever N the most recent vintage
        # publishes. Older vintages are dropped.
    """
    if df is None or len(df) == 0:
        return df
    if as_of_col not in df.columns:
        return df.iloc[0:0].copy()

    work = df.copy()
    work[as_of_col] = pd.to_datetime(work[as_of_col], errors="coerce")

    has_region = region_col is not None and region_col in work.columns
    if has_region:
        latest_per_region = work.groupby(region_col)[as_of_col].transform("max")
        work = work[work[as_of_col] == latest_per_region]
    else:
        global_max = work[as_of_col].max()
        if pd.isna(global_max):
            return work.iloc[0:0]
        work = work[work[as_of_col] == global_max]

    if len(work) == 0:
        return work.reset_index(drop=True)

    group_cols = ([region_col] if has_region else []) + [date_col]
    hour_counts = work.groupby(group_cols)[hour_col].nunique()
    covered_keys = hour_counts[hour_counts >= 24].index

    if has_region:
        idx = pd.MultiIndex.from_arrays([work[c] for c in group_cols])
        work = work[idx.isin(covered_keys)]
    else:
        work = work[work[date_col].isin(covered_keys)]

    return work.reset_index(drop=True)


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


def _normalize_lmps_da_sep(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the DA System Energy Price component from pjm_lmps_hourly.

    SEP is system-wide (uniform across nodes), but the parquet stores it
    per (hub, hour). We carry the ``hub`` (region) column through so callers
    can filter to a single row per (date, HE) — picking any one hub yields
    the same SEP series.
    """
    output = df.copy()
    if "market" in output.columns:
        output = output[output["market"].astype(str).str.lower() == "da"].copy()

    date_col = _first_present(output.columns, _DATE_CANDIDATES)
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    region_col = _first_present(output.columns, _REGION_CANDIDATES)
    sep_col = "lmp_system_energy_price"

    required = {
        "date": date_col,
        "hour_ending": hour_col,
        "region": region_col,
        "lmp_system_energy_price": sep_col if sep_col in output.columns else None,
    }
    missing = [name for name, column in required.items() if column is None]
    if missing:
        raise KeyError(
            f"Could not normalize lmps_da_sep; missing fields: {missing}. "
            f"Columns: {list(output.columns)}"
        )

    normalized = output[
        [
            required["date"],
            required["hour_ending"],
            required["region"],
            required["lmp_system_energy_price"],
        ]
    ].rename(columns={v: k for k, v in required.items()})
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["lmp_system_energy_price"] = pd.to_numeric(
        normalized["lmp_system_energy_price"], errors="coerce"
    )
    normalized["region"] = normalized["region"].astype(str)
    normalized = normalized.dropna(
        subset=["date", "hour_ending", "lmp_system_energy_price"]
    )
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    return normalized


def _normalize_lmps_rt(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()
    if "market" in output.columns:
        output = output[output["market"].astype(str).str.lower() == "rt"].copy()

    date_col = _first_present(output.columns, _DATE_CANDIDATES)
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    region_col = _first_present(output.columns, _REGION_CANDIDATES)
    price_col = _first_present(
        output.columns,
        ("lmp", "lmp_total", "rt_lmp_total", "rt_lmp"),
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
            f"Could not normalize lmps_rt; missing fields: {missing}. "
            f"Columns: {list(output.columns)}"
        )

    keep = [
        required["date"],
        required["hour_ending"],
        required["region"],
        required["lmp"],
    ]
    if "rt_source" in output.columns:
        keep.append("rt_source")
    normalized = output[keep].rename(columns={v: k for k, v in required.items()})
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["lmp"] = pd.to_numeric(normalized["lmp"], errors="coerce")
    normalized["region"] = normalized["region"].astype(str)
    normalized = normalized.dropna(subset=["date", "hour_ending", "lmp"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)

    # Prefer verified rows when verified+unverified overlap on the same key.
    if "rt_source" in normalized.columns:
        normalized["_verified_rank"] = (
            normalized["rt_source"].astype(str).str.lower() == "verified"
        ).astype(int)
        normalized = (
            normalized.sort_values("_verified_rank", ascending=False)
            .drop_duplicates(subset=["region", "date", "hour_ending"], keep="first")
            .drop(columns=["_verified_rank", "rt_source"])
        )
    return normalized.sort_values(["region", "date", "hour_ending"]).reset_index(
        drop=True
    )


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
        [
            required["date"],
            required["hour_ending"],
            required["region"],
            required["rt_load_mw"],
        ]
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

    keep = [
        required["date"],
        required["hour_ending"],
        required["region"],
        required["forecast_load_mw"],
    ]
    if "as_of_date" in output.columns:
        keep.append("as_of_date")
    if "forecast_execution_datetime_local" in output.columns:
        keep.append("forecast_execution_datetime_local")

    normalized = output[keep].rename(columns={v: k for k, v in required.items()})
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["forecast_load_mw"] = pd.to_numeric(
        normalized["forecast_load_mw"], errors="coerce"
    )
    normalized["region"] = normalized["region"].astype(str)
    normalized = normalized.dropna(subset=["date", "hour_ending", "forecast_load_mw"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    if "as_of_date" in normalized.columns:
        normalized["as_of_date"] = _coerce_date(normalized, "as_of_date")
    if "forecast_execution_datetime_local" in normalized.columns:
        normalized["forecast_execution_datetime_local"] = pd.to_datetime(
            normalized["forecast_execution_datetime_local"], errors="coerce"
        )
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
        if column not in metadata_columns
        and pd.api.types.is_numeric_dtype(output[column])
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
    date_col = _first_present(
        output.columns, ("date", "forecast_date", "forecast_execution_date")
    )
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
        normalized["forecast_execution_date"] = _coerce_date(
            normalized, "forecast_execution_date"
        )
        if "forecast_day_number" not in normalized.columns:
            date_ts = pd.to_datetime(normalized["date"], errors="coerce")
            exec_ts = pd.to_datetime(
                normalized["forecast_execution_date"], errors="coerce"
            )
            normalized["forecast_day_number"] = (date_ts - exec_ts).dt.days + 1
    for column in outage_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    if "forecast_day_number" in normalized.columns:
        normalized["forecast_day_number"] = pd.to_numeric(
            normalized["forecast_day_number"], errors="coerce"
        ).astype("Int64")
    if "forecast_rank" in normalized.columns:
        normalized["forecast_rank"] = pd.to_numeric(
            normalized["forecast_rank"], errors="coerce"
        ).astype("Int64")
    normalized = normalized.dropna(subset=["date"])
    return normalized


def _normalize_outages_forecast_history(df: pd.DataFrame) -> pd.DataFrame:
    """Thin pass-through for ``pjm_outages_forecast_history`` parquets.

    The dbt mart already emits clean column names — ``as_of_date`` /
    ``forecast_execution_date`` (synonymous), ``forecast_date``,
    ``lead_days``, ``region``, and the four MW columns. This normalizer
    just type-coerces dates and numerics and keeps every column.
    """
    output = df.copy()
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

    if (
        "forecast_date" not in output.columns
        or region_col is None
        or not outage_columns
    ):
        raise KeyError(
            "Could not normalize outages_forecast_history; expected "
            "forecast_date/region/outage columns. "
            f"Columns: {list(output.columns)}"
        )

    if region_col != "region":
        output = output.rename(columns={region_col: "region"})
    output["region"] = output["region"].astype(str)

    for date_col in ("as_of_date", "forecast_execution_date", "forecast_date"):
        if date_col in output.columns:
            output[date_col] = pd.to_datetime(output[date_col], errors="coerce").dt.date

    if "lead_days" in output.columns:
        output["lead_days"] = pd.to_numeric(
            output["lead_days"], errors="coerce"
        ).astype("Int64")

    for column in outage_columns:
        output[column] = pd.to_numeric(output[column], errors="coerce")

    output = output.dropna(subset=["forecast_date"])
    sort_cols = [
        c for c in ("region", "forecast_date", "as_of_date") if c in output.columns
    ]
    if sort_cols:
        output = output.sort_values(sort_cols).reset_index(drop=True)
    return output


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
    if "as_of_date" in output.columns:
        keep.append("as_of_date")
    if "forecast_execution_datetime_local" in output.columns:
        keep.append("forecast_execution_datetime_local")
    normalized = output[keep].rename(
        columns={date_col: "date", hour_col: "hour_ending", solar_col: "solar_forecast"}
    )
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["solar_forecast"] = pd.to_numeric(
        normalized["solar_forecast"], errors="coerce"
    )
    if "solar_forecast_btm" in normalized.columns:
        normalized["solar_forecast_btm"] = pd.to_numeric(
            normalized["solar_forecast_btm"], errors="coerce"
        )
    normalized = normalized.dropna(subset=["date", "hour_ending", "solar_forecast"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    if "as_of_date" in normalized.columns:
        normalized["as_of_date"] = _coerce_date(normalized, "as_of_date")
    if "forecast_execution_datetime_local" in normalized.columns:
        normalized["forecast_execution_datetime_local"] = pd.to_datetime(
            normalized["forecast_execution_datetime_local"], errors="coerce"
        )
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

    keep = [date_col, hour_col, wind_col]
    if "as_of_date" in output.columns:
        keep.append("as_of_date")
    if "forecast_execution_datetime_local" in output.columns:
        keep.append("forecast_execution_datetime_local")

    normalized = output[keep].rename(
        columns={date_col: "date", hour_col: "hour_ending", wind_col: "wind_forecast"}
    )
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["wind_forecast"] = pd.to_numeric(
        normalized["wind_forecast"], errors="coerce"
    )
    normalized = normalized.dropna(subset=["date", "hour_ending", "wind_forecast"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    if "as_of_date" in normalized.columns:
        normalized["as_of_date"] = _coerce_date(normalized, "as_of_date")
    if "forecast_execution_datetime_local" in normalized.columns:
        normalized["forecast_execution_datetime_local"] = pd.to_datetime(
            normalized["forecast_execution_datetime_local"], errors="coerce"
        )
    return normalized


def _normalize_weather_hourly(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()

    if "region" in output.columns:
        pjm_rows = output["region"].astype(str).str.upper() == "PJM"
        if pjm_rows.any():
            output = output[pjm_rows].copy()

    date_col = _first_present(
        output.columns, ("date_ept", "forecast_date_ept", "forecast_date", "date")
    )
    hour_col = _first_present(output.columns, ("hour_ending_ept", "hour_ending"))
    temp_col = _first_present(
        output.columns, ("temperature", "temp", "temperature_f", "temp_f")
    )
    # WSI forecast parquet carries the vintage stamp; observed does not.
    exec_col = _first_present(
        output.columns,
        ("forecast_execution_datetime_utc", "forecast_execution_datetime"),
    )

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
        "dew_point_temp": _first_present(
            output.columns, ("dewpoint", "dew_point_temp", "dew_point")
        ),
        "wind_speed_mph": _first_present(
            output.columns, ("wind_speed", "wind_speed_mph")
        ),
        "relative_humidity": _first_present(
            output.columns, ("relative_humidity", "humidity")
        ),
        "cloud_cover_pct": _first_present(
            output.columns, ("cloud_cover_pct", "cloud_cover")
        ),
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

    numeric_cols = [
        column for column in normalized.columns if column not in ("date", "hour_ending")
    ]
    for column in numeric_cols:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized = normalized.dropna(subset=["date", "hour_ending", "temp"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)

    # WSI has one row per station-hour. Aggregate to PJM date-hour.
    normalized = normalized.groupby(["date", "hour_ending"], as_index=False).mean(
        numeric_only=True
    )

    # Re-attach the forecast vintage stamp (one per date), if present. Kept
    # out of the numeric groupby above so it survives as a datetime.
    if exec_col is not None:
        execs = output[[date_col, exec_col]].rename(
            columns={date_col: "date", exec_col: "forecast_executed_utc"}
        )
        execs["date"] = _coerce_date(execs, "date")
        execs["forecast_executed_utc"] = pd.to_datetime(
            execs["forecast_executed_utc"], errors="coerce"
        )
        execs = (
            execs.dropna(subset=["date"])
            .groupby("date", as_index=False)["forecast_executed_utc"]
            .max()
        )
        normalized = normalized.merge(execs, on="date", how="left")

    return normalized


def _normalize_gas_prices_hourly(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()

    date_col = _first_present(output.columns, ("date", "gas_day", "forecast_date"))
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    if hour_col is None and "datetime" in output.columns:
        output["hour_ending"] = (
            pd.to_datetime(output["datetime"], errors="coerce").dt.hour + 1
        )
        hour_col = "hour_ending"

    if date_col is None or hour_col is None:
        raise KeyError(
            "Could not normalize gas_prices_hourly; expected date/hour columns. "
            f"Columns: {list(output.columns)}"
        )

    hub_map = {
        "gas_m3": _first_present(
            output.columns, ("gas_m3", "tetco_m3_cash", "gas_m3_price")
        ),
        "gas_tco": _first_present(output.columns, ("gas_tco", "columbia_tco_cash")),
        "gas_tz6": _first_present(output.columns, ("gas_tz6", "transco_z6_ny_cash")),
        "gas_dom_south": _first_present(
            output.columns,
            ("gas_dom_south", "dominion_south_cash"),
        ),
    }

    keep_hub_columns = [
        source_col for source_col in hub_map.values() if source_col is not None
    ]
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
    normalized = normalized.groupby(["date", "hour_ending"], as_index=False).mean(
        numeric_only=True
    )
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

    if (
        date_col is None
        or hour_col is None
        or region_col is None
        or value_col not in output.columns
    ):
        raise KeyError(
            f"Could not normalize meteologica frame for {value_col!r}; "
            f"columns: {list(output.columns)}"
        )

    keep = [date_col, hour_col, region_col, value_col]
    if "as_of_date" in output.columns:
        keep.append("as_of_date")
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
    if "as_of_date" in normalized.columns:
        normalized["as_of_date"] = _coerce_date(normalized, "as_of_date")
    if "forecast_execution_datetime_local" in normalized.columns:
        normalized["forecast_execution_datetime_local"] = pd.to_datetime(
            normalized["forecast_execution_datetime_local"], errors="coerce"
        )

    # Historical parquets carry one row per (as_of_date, region, date, he); the
    # live mart has no as_of_date and uses forecast_rank to pick the latest.
    if "forecast_rank" in normalized.columns and "as_of_date" not in normalized.columns:
        normalized["forecast_rank"] = pd.to_numeric(
            normalized["forecast_rank"], errors="coerce"
        )
        normalized = normalized.sort_values("forecast_rank", ascending=False)
        normalized = normalized.drop_duplicates(
            subset=["region", "date", "hour_ending"], keep="first"
        )

    sort_cols = ["region", "date", "hour_ending"]
    if "as_of_date" in normalized.columns:
        sort_cols = ["as_of_date", *sort_cols]
    return normalized.sort_values(sort_cols).reset_index(drop=True)


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

    if (
        date_col is None
        or hour_col is None
        or region_col is None
        or net_load_col is None
    ):
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
    return normalized.sort_values(["region", "date", "hour_ending"]).reset_index(
        drop=True
    )


def _normalize_meteologica_net_load(df: pd.DataFrame) -> pd.DataFrame:
    """Meteologica net_load combines load/solar/wind/net_load per (region, date, he)."""
    output = df.copy()
    date_col = _first_present(output.columns, ("forecast_date", "date"))
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)
    region_col = _first_present(output.columns, _REGION_CANDIDATES)
    value_cols = [
        c
        for c in (
            "forecast_load_mw",
            "solar_forecast",
            "wind_forecast",
            "net_load_forecast_mw",
        )
        if c in output.columns
    ]
    if date_col is None or hour_col is None or region_col is None or not value_cols:
        raise KeyError(
            "Could not normalize meteologica_net_load_forecast; "
            f"columns: {list(output.columns)}"
        )
    exec_datetime_cols = [
        c
        for c in (
            "load_forecast_execution_datetime_local",
            "solar_forecast_execution_datetime_local",
            "wind_forecast_execution_datetime_local",
        )
        if c in output.columns
    ]
    keep = [date_col, hour_col, region_col, *value_cols, *exec_datetime_cols]
    if "as_of_date" in output.columns:
        keep.append("as_of_date")
    normalized = output[keep].rename(
        columns={date_col: "date", hour_col: "hour_ending", region_col: "region"}
    )
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    normalized["region"] = normalized["region"].astype(str)
    for col in value_cols:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
    for col in exec_datetime_cols:
        normalized[col] = pd.to_datetime(normalized[col], errors="coerce")
    normalized = normalized.dropna(subset=["date", "hour_ending"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    if "as_of_date" in normalized.columns:
        normalized["as_of_date"] = _coerce_date(normalized, "as_of_date")
    sort_cols = ["region", "date", "hour_ending"]
    if "as_of_date" in normalized.columns:
        sort_cols = ["as_of_date", *sort_cols]
    return normalized.sort_values(sort_cols).reset_index(drop=True)


def _normalize_meteologica_da_price(df: pd.DataFrame) -> pd.DataFrame:
    """Meteologica PJM Western-Hub DA price forecast (deterministic + ENS).

    Source: ``meteologica_pjm_da_price_forecast_hourly_da_cutoff_historical``
    — the dbt mart joins Meteologica's deterministic point and ECMWF
    ensemble products at Western Hub, each independently DA-cutoff-vintage
    selected per as_of_date. Single price node, no region dim.

    Output keeps the four named summary series (deterministic + ens
    avg/bottom/top), the 51 individual ECMWF ensemble members
    (``da_price_ens_00`` .. ``da_price_ens_50``) when present in the source,
    and per-side execution metadata. Returned columns: as_of_date, date,
    hour_ending, da_price_deterministic, da_price_ens_average,
    da_price_ens_bottom, da_price_ens_top,
    det_forecast_execution_datetime_local,
    ens_forecast_execution_datetime_local, da_price_ens_00, ...,
    da_price_ens_50.
    """
    output = df.copy()
    date_col = _first_present(output.columns, ("forecast_date", "date"))
    hour_col = _first_present(output.columns, _HOUR_CANDIDATES)

    value_cols = [
        c
        for c in (
            "da_price_deterministic",
            "da_price_ens_average",
            "da_price_ens_bottom",
            "da_price_ens_top",
        )
        if c in output.columns
    ]
    if date_col is None or hour_col is None or not value_cols:
        raise KeyError(
            "Could not normalize meteologica_da_price_forecast; "
            f"columns: {list(output.columns)}"
        )

    member_prefix = "da_price_ens_"
    member_cols = sorted(
        c
        for c in output.columns
        if c.startswith(member_prefix) and c[len(member_prefix) :].isdigit()
    )

    exec_cols = [
        c
        for c in (
            "det_forecast_execution_datetime_local",
            "ens_forecast_execution_datetime_local",
        )
        if c in output.columns
    ]
    keep = [date_col, hour_col, *value_cols, *exec_cols, *member_cols]
    if "as_of_date" in output.columns:
        keep.append("as_of_date")

    normalized = output[keep].rename(
        columns={date_col: "date", hour_col: "hour_ending"}
    )
    normalized["date"] = _coerce_date(normalized, "date")
    normalized["hour_ending"] = _coerce_hour(normalized, "hour_ending")
    for col in (*value_cols, *member_cols):
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
    for col in exec_cols:
        normalized[col] = pd.to_datetime(normalized[col], errors="coerce")
    normalized = normalized.dropna(subset=["date", "hour_ending"])
    normalized["hour_ending"] = normalized["hour_ending"].astype(int)
    if "as_of_date" in normalized.columns:
        normalized["as_of_date"] = _coerce_date(normalized, "as_of_date")

    sort_cols = ["date", "hour_ending"]
    if "as_of_date" in normalized.columns:
        sort_cols = ["as_of_date", *sort_cols]
    return normalized.sort_values(sort_cols).reset_index(drop=True)


def _normalize_installed_capacity(df: pd.DataFrame) -> pd.DataFrame:
    """Energy Aspects monthly PJM installed capacity, fuel-disaggregated.

    Handles both column conventions:
      - new dbt mart:  natural_gas_mw, coal_mw, nuclear_mw, oil_products_mw,
                       solar_mw, onshore_wind_mw, offshore_wind_mw, hydro_mw,
                       battery_mw, total_installed_capacity_mw
      - legacy view:   ng_capacity_mw, coal_capacity_mw, ..., battery_capacity_mw
    """
    output = df.copy()
    date_col = _first_present(output.columns, _DATE_CANDIDATES)
    if date_col is None:
        raise KeyError(
            "Could not normalize installed_capacity; expected a date column. "
            f"Columns: {list(output.columns)}"
        )

    # Map any source naming -> canonical fuel column names.
    fuel_aliases = {
        "natural_gas_mw": ("natural_gas_mw", "ng_mw", "ng_capacity_mw"),
        "coal_mw": ("coal_mw", "coal_capacity_mw"),
        "nuclear_mw": ("nuclear_mw", "nuclear_capacity_mw"),
        "oil_products_mw": ("oil_products_mw", "oil_mw", "oil_capacity_mw"),
        "solar_mw": ("solar_mw", "solar_capacity_mw"),
        "onshore_wind_mw": ("onshore_wind_mw", "onshore_wind_capacity_mw"),
        "offshore_wind_mw": ("offshore_wind_mw", "offshore_wind_capacity_mw"),
        "hydro_mw": ("hydro_mw", "hydro_capacity_mw"),
        "battery_mw": ("battery_mw", "battery_capacity_mw"),
    }

    rename_map: dict[str, str] = {}
    for canonical, candidates in fuel_aliases.items():
        for cand in candidates:
            if cand in output.columns and cand != canonical:
                rename_map[cand] = canonical
                break

    normalized = output.rename(columns={date_col: "date", **rename_map})
    normalized["date"] = _coerce_date(normalized, "date")

    fuel_columns = [c for c in fuel_aliases if c in normalized.columns]
    if not fuel_columns:
        raise KeyError(
            "Could not normalize installed_capacity; no fuel columns found. "
            f"Columns: {list(output.columns)}"
        )
    for col in fuel_columns:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")

    # Prefer the dbt-computed total if present; otherwise sum the fuel cols.
    if "total_installed_capacity_mw" in normalized.columns:
        normalized["total_installed_capacity_mw"] = pd.to_numeric(
            normalized["total_installed_capacity_mw"],
            errors="coerce",
        )
    else:
        normalized["total_installed_capacity_mw"] = normalized[fuel_columns].sum(
            axis=1,
            min_count=1,
        )

    keep = ["date", *fuel_columns, "total_installed_capacity_mw"]
    normalized = normalized[keep].dropna(subset=["date"])
    normalized = normalized.sort_values("date").reset_index(drop=True)
    return normalized


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

    normalized = output[[date_col, *capacity_columns]].rename(
        columns={date_col: "date"}
    )
    normalized["date"] = _coerce_date(normalized, "date")
    for column in capacity_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized = (
        normalized.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    )
    return normalized


_PJM_DATES_DAILY_COLS: tuple[str, ...] = (
    "date",
    "day_of_week_number",
    "is_weekend",
    "is_nerc_holiday",
    "is_federal_holiday",
    "summer_winter",
    "holiday_name",
)


def _normalize_pjm_dates_daily(df: pd.DataFrame) -> pd.DataFrame:
    """PJM calendar metadata, one row per delivery date.

    day_of_week_number uses Sun=0..Sat=6 (PJM convention), not Python's Mon=0.
    """
    output = df.copy()
    keep = [c for c in _PJM_DATES_DAILY_COLS if c in output.columns]
    if "date" not in keep:
        raise KeyError(
            "Could not normalize pjm_dates_daily; expected 'date' column. "
            f"Columns: {list(output.columns)}"
        )
    output = output[keep]

    output["date"] = _coerce_date(output, "date")
    if "day_of_week_number" in output.columns:
        output["day_of_week_number"] = pd.to_numeric(
            output["day_of_week_number"],
            errors="coerce",
        ).astype("Int64")
    for flag in ("is_weekend", "is_nerc_holiday", "is_federal_holiday"):
        if flag in output.columns:
            output[flag] = (
                pd.to_numeric(output[flag], errors="coerce").fillna(0).astype(int)
            )
    if "summer_winter" in output.columns:
        output["summer_winter"] = (
            output["summer_winter"].astype("string").str.upper().fillna("")
        )

    output = output.dropna(subset=["date"]).drop_duplicates(
        subset=["date"], keep="last"
    )
    return output.sort_values("date").reset_index(drop=True)


def _normalize_reserve_market_results_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """PJM cleared operating reserves (system-wide PJM_RTO locale), wide
    per (date, hour_ending).

    The dbt mart ``pjm_reserve_market_results_hourly`` is already clean
    (one row per delivery hour, per-service cleared MW / requirement / MCP
    columns, plus derived ``operating_reserve_mw_cleared`` /
    ``operating_reserve_requirement_mw`` / ``reserve_scarcity_flag``).
    This normalizer just type-coerces ``date`` / ``hour_ending`` and
    drops malformed rows. Backward-only: today and forward dates won't
    have rows; forward-looking consumers compute a rolling profile.
    """
    output = df.copy()
    if "date" not in output.columns or "hour_ending" not in output.columns:
        raise KeyError(
            "Could not normalize reserve_market_results_hourly; expected date/hour columns. "
            f"Columns: {list(output.columns)}"
        )
    output["date"] = _coerce_date(output, "date")
    output["hour_ending"] = _coerce_hour(output, "hour_ending")
    output = output.dropna(subset=["date", "hour_ending"])
    output["hour_ending"] = output["hour_ending"].astype(int)
    for col in (
        "sr_total_mw",
        "sr_requirement_mw",
        "sr_mcp",
        "pr_total_mw",
        "pr_requirement_mw",
        "pr_mcp",
        "pr_nsr_mw",
        "min30_total_mw",
        "min30_requirement_mw",
        "min30_mcp",
        "reg_total_mw",
        "reg_requirement_mw",
        "reg_ccp",
        "reg_pcp",
        "operating_reserve_mw_cleared",
        "operating_reserve_requirement_mw",
    ):
        if col in output.columns:
            output[col] = pd.to_numeric(output[col], errors="coerce")
    if "reserve_scarcity_flag" in output.columns:
        output["reserve_scarcity_flag"] = output["reserve_scarcity_flag"].astype(bool)
    return output.sort_values(["date", "hour_ending"]).reset_index(drop=True)


_NORMALIZERS = {
    "lmps_da": _normalize_lmps_da,
    "lmps_da_sep": _normalize_lmps_da_sep,
    "lmps_rt": _normalize_lmps_rt,
    "load_rt": _normalize_load_rt,
    "load_forecast": _normalize_load_forecast,
    "fuel_mix": _normalize_fuel_mix,
    "outages_actual": _normalize_outages_actual,
    "outages_forecast": _normalize_outages_forecast,
    "outages_forecast_history": _normalize_outages_forecast_history,
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
    "meteologica_da_price_forecast": _normalize_meteologica_da_price,
    "pjm_net_load_forecast": _normalize_meteologica_net_load,
    "net_load_forecast": _normalize_meteologica_net_load,
    "net_load_actual": _normalize_net_load_actual,
    "day_gen_capacity": _normalize_day_gen_capacity,
    "installed_capacity": _normalize_installed_capacity,
    "pjm_dates_daily": _normalize_pjm_dates_daily,
    "reserve_market_results_hourly": _normalize_reserve_market_results_hourly,
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


def load_lmps_rt(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """PJM RT (settled) hourly LMPs. Verified rows preferred over unverified."""
    return _load_dataset("lmps_rt", path=path, cache_dir=cache_dir, columns=columns)


def load_lmp_system_energy_da(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """PJM DA System Energy Price (SEP) component of LMP.

    Returns columns: date, hour_ending, region, lmp_system_energy_price.
    SEP is system-wide; the parquet stores it per (hub, hour), so filter
    to a single hub to obtain one row per (date, HE).
    """
    return _load_dataset("lmps_da_sep", path=path, cache_dir=cache_dir, columns=columns)


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
    lead_days: int | None = 1,
    latest_only: bool = False,
) -> pd.DataFrame:
    """PJM DA-cutoff load forecast.

    The historical mart carries seven vintages per (region, date, hour_ending) — one
    per as_of_date from forecast_date - 6 through forecast_date itself.

    Two modes (mutually exclusive — ``latest_only=True`` ignores ``lead_days``):

      - ``lead_days=N`` (default ``lead_days=1``): pick the vintage where
        ``as_of_date == forecast_date - N``. ``lead_days=None`` returns all
        vintages. The filter is a no-op on parquets without ``as_of_date``.
      - ``latest_only=True``: surface only the most-recent ``as_of_date`` per
        region, dropping (region, forecast_date) tuples that don't have all
        24 ``hour_ending`` values under that vintage. Lets downstream callers
        consume the full multi-day horizon a single publish window covers.
    """
    df = _load_dataset("load_forecast", path=path, cache_dir=cache_dir, columns=None)
    if latest_only:
        df = _filter_to_latest_vintage_full_coverage(df, region_col="region")
    elif lead_days is not None and "as_of_date" in df.columns:
        delta = (
            pd.to_datetime(df["date"], errors="coerce")
            - pd.to_datetime(df["as_of_date"], errors="coerce")
        ).dt.days
        df = df[delta == lead_days].copy()
    return _apply_column_filter(df, columns)


def load_load_coalesced(
    *,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    latest_only: bool = False,
) -> pd.DataFrame:
    """Forecast-first hourly load — the unified series models should consume.

    Two modes:

      - Default (``latest_only=False``): for each (region, date), the
        DA-cutoff forecast (lead_days=1) wins when the historical parquet
        has all 24 hour_ending values; RT actuals fill every other
        (region, date), including pre-backfill dates and any forecast day
        with incomplete hourly coverage.
      - ``latest_only=True``: forecast frame uses the most-recent
        ``as_of_date`` per region (full multi-day horizon, drop dates
        with partial coverage); RT still fills historical (region, date)
        tuples that lack forecast coverage. (region, date) tuples beyond
        the RT mart's max date with no forecast coverage do not appear.

    Strict 24-hour completeness gate so daily aggregations (peak / valley
    / ramps) never compose forecast and RT hours within the same date.

    Returns columns: as_of_date, date, hour_ending, region, load_mw,
    forecast_execution_datetime_local, source ('forecast'|'rt').
    ``as_of_date`` is NaT for RT rows.
    """
    fcst_full = load_load_forecast(cache_dir=cache_dir, latest_only=latest_only)
    fcst_cols = [
        "date",
        "hour_ending",
        "region",
        "forecast_load_mw",
        "forecast_execution_datetime_local",
    ]
    if "as_of_date" in fcst_full.columns:
        fcst_cols = ["as_of_date"] + fcst_cols
    fcst = fcst_full[fcst_cols].rename(columns={"forecast_load_mw": "load_mw"})
    if "as_of_date" not in fcst.columns:
        fcst["as_of_date"] = pd.NaT
    rt = (
        load_load_rt(cache_dir=cache_dir)[
            ["date", "hour_ending", "region", "rt_load_mw"]
        ]
        .rename(columns={"rt_load_mw": "load_mw"})
        .assign(forecast_execution_datetime_local=pd.NaT, as_of_date=pd.NaT)
    )

    # A (region, date) is "forecast-covered" only when all 24 HEs are present.
    hour_counts = fcst.groupby(["region", "date"])["hour_ending"].nunique()
    covered_keys = hour_counts[hour_counts >= 24].index

    fcst_idx = pd.MultiIndex.from_arrays([fcst["region"], fcst["date"]])
    fcst_kept = fcst[fcst_idx.isin(covered_keys)].assign(source="forecast")

    rt_idx = pd.MultiIndex.from_arrays([rt["region"], rt["date"]])
    rt_fallback = rt[~rt_idx.isin(covered_keys)].assign(source="rt")

    out_cols = [
        "as_of_date",
        "date",
        "hour_ending",
        "region",
        "load_mw",
        "forecast_execution_datetime_local",
        "source",
    ]
    out = (
        pd.concat([fcst_kept[out_cols], rt_fallback[out_cols]], ignore_index=True)
        .sort_values(["region", "date", "hour_ending"])
        .reset_index(drop=True)
    )
    return _apply_column_filter(out, columns)


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
    return _load_dataset(
        "outages_actual", path=path, cache_dir=cache_dir, columns=columns
    )


def load_outages_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "outages_forecast", path=path, cache_dir=cache_dir, columns=columns
    )


def load_outages_forecast_history(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    lead_days: int | None = 1,
) -> pd.DataFrame:
    """PJM outages forecast history (back to 2020).

    Carries one row per (region, forecast_date, as_of_date) — PJM publishes
    once per morning, so each (region, forecast_date) has up to 8 vintages
    with ``lead_days`` 0..7. Default ``lead_days=1`` returns the DA
    decision-time vintage (the forecast on file at the DA cutoff for the
    operating day, i.e. ``as_of_date == forecast_date - 1``). Pass
    ``lead_days=None`` to return all vintages, or another int (0-7) for a
    different lead. The filter is on the ``lead_days`` column directly
    (the dbt view computes it), so no date math is needed here.
    """
    df = _load_dataset(
        "outages_forecast_history",
        path=path,
        cache_dir=cache_dir,
        columns=None,
    )
    if lead_days is not None and "lead_days" in df.columns:
        df = df[df["lead_days"] == lead_days].copy()
    return _apply_column_filter(df, columns)


def load_solar_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "solar_forecast", path=path, cache_dir=cache_dir, columns=columns
    )


def load_solar_coalesced(
    *,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    latest_only: bool = False,
) -> pd.DataFrame:
    """Forecast-first hourly RTO solar — the unified series models should consume.

    Two modes:

      - Default (``latest_only=False``): for each date, the PJM DA-cutoff
        solar forecast (lead_days=1, i.e. as_of_date == forecast_date - 1)
        wins when the historical parquet has all 24 hour_ending values; RT
        actuals from pjm_net_load_rt_hourly (filtered to region=RTO,
        solar_gen_mw) fill every other date.
      - ``latest_only=True``: pick the most-recent ``as_of_date`` (system-
        wide — PJM solar forecast has no region) and surface every
        forecast_date with full 24-HE coverage. RT still fills historical
        dates that lack forecast coverage.

    Strict 24-HE rule applies in both modes — daily aggregations never
    compose forecast and RT hours within the same date.

    PJM solar forecast is system-wide (no region column) and actuals are
    filtered to RTO, so the output is RTO-only with no region column.

    Pre-2019-04-02 dates have no solar_gen_mw (PJM did not publish solar
    actuals before that date) and no forecast coverage either, so they are
    naturally absent from the coalesced series.

    Returns columns: as_of_date, date, hour_ending, solar_mw,
    forecast_execution_datetime_local, source ('forecast'|'rt').
    ``as_of_date`` is NaT for RT rows.
    """
    fcst_full = load_solar_forecast(cache_dir=cache_dir)
    if latest_only:
        fcst_full = _filter_to_latest_vintage_full_coverage(fcst_full, region_col=None)
    elif "as_of_date" in fcst_full.columns:
        delta = (
            pd.to_datetime(fcst_full["date"], errors="coerce")
            - pd.to_datetime(fcst_full["as_of_date"], errors="coerce")
        ).dt.days
        fcst_full = fcst_full[delta == 1].copy()
    fcst_cols = [
        "date",
        "hour_ending",
        "solar_forecast",
        "forecast_execution_datetime_local",
    ]
    if "as_of_date" in fcst_full.columns:
        fcst_cols = ["as_of_date"] + fcst_cols
    fcst = fcst_full[fcst_cols].rename(columns={"solar_forecast": "solar_mw"})
    if "as_of_date" not in fcst.columns:
        fcst["as_of_date"] = pd.NaT

    rt_full = load_net_load_actuals(cache_dir=cache_dir)
    rt = (
        rt_full[rt_full["region"].astype(str) == "RTO"][
            ["date", "hour_ending", "solar_gen_mw"]
        ]
        .rename(columns={"solar_gen_mw": "solar_mw"})
        .dropna(subset=["solar_mw"])
        .assign(forecast_execution_datetime_local=pd.NaT, as_of_date=pd.NaT)
    )

    # A date is "forecast-covered" only when all 24 HEs are present.
    hour_counts = fcst.groupby("date")["hour_ending"].nunique()
    covered_dates = hour_counts[hour_counts >= 24].index

    fcst_kept = fcst[fcst["date"].isin(covered_dates)].assign(source="forecast")
    rt_fallback = rt[~rt["date"].isin(covered_dates)].assign(source="rt")

    out_cols = [
        "as_of_date",
        "date",
        "hour_ending",
        "solar_mw",
        "forecast_execution_datetime_local",
        "source",
    ]
    out = (
        pd.concat([fcst_kept[out_cols], rt_fallback[out_cols]], ignore_index=True)
        .sort_values(["date", "hour_ending"])
        .reset_index(drop=True)
    )
    return _apply_column_filter(out, columns)


def load_wind_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "wind_forecast", path=path, cache_dir=cache_dir, columns=columns
    )


def load_wind_coalesced(
    *,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    latest_only: bool = False,
) -> pd.DataFrame:
    """Forecast-first hourly RTO wind — the unified series models should consume.

    Two modes:

      - Default (``latest_only=False``): for each date, the PJM DA-cutoff
        wind forecast (lead_days=1, i.e. as_of_date == forecast_date - 1)
        wins when the historical parquet has all 24 hour_ending values;
        RT actuals from pjm_net_load_rt_hourly (filtered to region=RTO,
        wind_gen_mw) fill every other date.
      - ``latest_only=True``: pick the most-recent ``as_of_date`` (system-
        wide — PJM wind forecast has no region) and surface every
        forecast_date with full 24-HE coverage. RT still fills historical
        dates that lack forecast coverage.

    Strict 24-HE rule applies in both modes — daily aggregations
    (daily avg / max / intraday std) never compose forecast and RT hours
    within the same date.

    PJM wind forecast is system-wide (no region column) and actuals are
    filtered to RTO, so the output is RTO-only with no region column.

    Returns columns: as_of_date, date, hour_ending, wind_mw,
    forecast_execution_datetime_local, source ('forecast'|'rt').
    ``as_of_date`` is NaT for RT rows.
    """
    fcst_full = load_wind_forecast(cache_dir=cache_dir)
    if latest_only:
        fcst_full = _filter_to_latest_vintage_full_coverage(fcst_full, region_col=None)
    elif "as_of_date" in fcst_full.columns:
        delta = (
            pd.to_datetime(fcst_full["date"], errors="coerce")
            - pd.to_datetime(fcst_full["as_of_date"], errors="coerce")
        ).dt.days
        fcst_full = fcst_full[delta == 1].copy()
    fcst_cols = [
        "date",
        "hour_ending",
        "wind_forecast",
        "forecast_execution_datetime_local",
    ]
    if "as_of_date" in fcst_full.columns:
        fcst_cols = ["as_of_date"] + fcst_cols
    fcst = fcst_full[fcst_cols].rename(columns={"wind_forecast": "wind_mw"})
    if "as_of_date" not in fcst.columns:
        fcst["as_of_date"] = pd.NaT

    rt_full = load_net_load_actuals(cache_dir=cache_dir)
    rt = (
        rt_full[rt_full["region"].astype(str) == "RTO"][
            ["date", "hour_ending", "wind_gen_mw"]
        ]
        .rename(columns={"wind_gen_mw": "wind_mw"})
        .dropna(subset=["wind_mw"])
        .assign(forecast_execution_datetime_local=pd.NaT, as_of_date=pd.NaT)
    )

    # A date is "forecast-covered" only when all 24 HEs are present.
    hour_counts = fcst.groupby("date")["hour_ending"].nunique()
    covered_dates = hour_counts[hour_counts >= 24].index

    fcst_kept = fcst[fcst["date"].isin(covered_dates)].assign(source="forecast")
    rt_fallback = rt[~rt["date"].isin(covered_dates)].assign(source="rt")

    out_cols = [
        "as_of_date",
        "date",
        "hour_ending",
        "wind_mw",
        "forecast_execution_datetime_local",
        "source",
    ]
    out = (
        pd.concat([fcst_kept[out_cols], rt_fallback[out_cols]], ignore_index=True)
        .sort_values(["date", "hour_ending"])
        .reset_index(drop=True)
    )
    return _apply_column_filter(out, columns)


def load_net_load_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "net_load_forecast", path=path, cache_dir=cache_dir, columns=columns
    )


def load_pjm_net_load_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """PJM-native net-load forecast (no Meteologica fallback).

    Returns load/solar/wind/net_load_forecast_mw per (region, date, hour_ending),
    with as_of_date when present in the historical mart.
    """
    return _load_dataset(
        "pjm_net_load_forecast",
        path=path,
        cache_dir=cache_dir,
        columns=columns,
    )


def load_net_load_actuals(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "net_load_actual", path=path, cache_dir=cache_dir, columns=columns
    )


def load_pjm_net_load_coalesced(
    *,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    lead_days: int | None = 1,
    latest_only: bool = False,
    region: str = "RTO",
) -> pd.DataFrame:
    """Forecast-first PJM net-load — strict 24-HE coverage gate.

    Superseded for new code by ``load_pjm_supply_demand_coalesced``, which
    returns the full bundle (load + solar + wind + net_load) under the
    same single-source-decision rule. This function remains for callers
    that only need ``net_load_mw`` and want to skip the wider frame.
    Output is internally consistent on its own; do NOT compose
    ``net_load_mw`` from this function with values from
    ``load_solar_coalesced`` / ``load_wind_coalesced`` — the per-series
    coalescers can disagree on forecast-vs-RT and break the identity
    ``net_load = load - solar - wind``. Use the unified loader instead.

    PJM publishes the net-load forecast for RTO only; RT actuals are
    filtered to ``region`` (default RTO) to match. Mirrors
    ``load_load_coalesced`` semantics for net-load (load minus reported
    solar + wind).

    Two modes (mutually exclusive — ``latest_only=True`` ignores ``lead_days``):

      - ``lead_days=N`` (default ``lead_days=1``): pick the DA-cutoff
        vintage (``as_of_date == date - N``). ``None`` skips the filter.
      - ``latest_only=True``: pick the most-recent ``as_of_date`` (per
        region) and surface every forecast_date with full 24-HE coverage.

    24-HE coverage gate applies in both modes. RT fills (region, date)
    tuples that lack forecast coverage; tuples beyond the RT mart's max
    date with no forecast coverage do not appear.

    Returns columns: as_of_date, date, hour_ending, region, net_load_mw,
    forecast_execution_datetime_local, source ('forecast'|'rt').
    ``as_of_date`` is NaT for RT rows.
    """
    fcst_full = load_pjm_net_load_forecast(cache_dir=cache_dir)
    fcst_full = fcst_full[fcst_full["region"].astype(str) == region].copy()
    if latest_only:
        fcst_full = _filter_to_latest_vintage_full_coverage(
            fcst_full, region_col="region"
        )
    elif lead_days is not None and "as_of_date" in fcst_full.columns:
        delta = (
            pd.to_datetime(fcst_full["date"], errors="coerce")
            - pd.to_datetime(fcst_full["as_of_date"], errors="coerce")
        ).dt.days
        fcst_full = fcst_full[delta == lead_days].copy()

    fcst_cols = [
        "date",
        "hour_ending",
        "region",
        "net_load_forecast_mw",
        "load_forecast_execution_datetime_local",
    ]
    if "as_of_date" in fcst_full.columns:
        fcst_cols = ["as_of_date"] + fcst_cols
    fcst = fcst_full[fcst_cols].rename(
        columns={
            "net_load_forecast_mw": "net_load_mw",
            "load_forecast_execution_datetime_local": "forecast_execution_datetime_local",
        }
    )
    if "as_of_date" not in fcst.columns:
        fcst["as_of_date"] = pd.NaT
    rt = (
        load_net_load_actuals(cache_dir=cache_dir)[
            ["date", "hour_ending", "region", "net_load_mw"]
        ]
        .pipe(lambda d: d[d["region"].astype(str) == region])
        .dropna(subset=["net_load_mw"])
        .assign(forecast_execution_datetime_local=pd.NaT, as_of_date=pd.NaT)
    )

    hour_counts = fcst.groupby(["region", "date"])["hour_ending"].nunique()
    covered_keys = hour_counts[hour_counts >= 24].index

    fcst_idx = pd.MultiIndex.from_arrays([fcst["region"], fcst["date"]])
    fcst_kept = fcst[fcst_idx.isin(covered_keys)].assign(source="forecast")

    rt_idx = pd.MultiIndex.from_arrays([rt["region"], rt["date"]])
    rt_fallback = rt[~rt_idx.isin(covered_keys)].assign(source="rt")

    out_cols = [
        "as_of_date",
        "date",
        "hour_ending",
        "region",
        "net_load_mw",
        "forecast_execution_datetime_local",
        "source",
    ]
    out = (
        pd.concat([fcst_kept[out_cols], rt_fallback[out_cols]], ignore_index=True)
        .sort_values(["region", "date", "hour_ending"])
        .reset_index(drop=True)
    )
    return _apply_column_filter(out, columns)


def load_pjm_supply_demand_coalesced(
    *,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    region: str = "RTO",
    lead_days: int | None = 1,
    latest_only: bool = False,
) -> pd.DataFrame:
    """Forecast-first hourly RTO supply-demand bundle: load + solar + wind + net_load.

    Unified single-source decision per (region, date) for ALL FOUR series
    simultaneously. A (region, date) is 'forecast' iff the DA-cutoff
    net_load forecast has full 24-hour coverage (which — by upstream
    construction in dbt's ``pjm_net_load_forecast_hourly_da_cutoff`` INNER
    JOIN of the three component forecasts — implies load+solar+wind are
    all present). Otherwise the date falls back to RT for all four.

    Eliminates the cross-source mixing artifact where the per-series
    coalescers (``load_load_coalesced``, ``load_solar_coalesced``,
    ``load_wind_coalesced``, ``load_pjm_net_load_coalesced``) each
    independently decide forecast-vs-RT. On dates where load+net_load fall
    back to RT but solar+wind stay on forecast, the identity
    ``net_load = load - solar - wind`` breaks (e.g. 2025-05-01: ~3.9 GW
    max gap, ~932 MW mean). This unified function holds the identity by
    construction:

      - Forecast rows come from ``pjm_net_load_forecast_hourly_da_cutoff``
        (built upstream by INNER JOIN of the three component forecasts).
      - RT rows come from ``pjm_net_load_rt_hourly`` (built upstream by
        LEFT JOIN of the three component RT marts).

    PJM solar/wind forecasts are system-wide; net_load forecast is
    RTO-only. ``region`` defaults to and currently only supports 'RTO' —
    sub-zonal load (MIDATL/WEST/SOUTH) lacks matching renewable forecasts.
    Use ``load_load_coalesced`` for sub-zonal load alone.

    Two modes (mutually exclusive — ``latest_only=True`` ignores ``lead_days``):

      - ``lead_days=N`` (default ``lead_days=1``): pick the DA-cutoff
        vintage (``as_of_date == forecast_date - N``). ``None`` skips
        the filter.
      - ``latest_only=True``: pick the most-recent ``as_of_date`` (per
        region) and surface every forecast_date with full 24-HE
        coverage — exposes the full multi-day horizon a single publish
        window covers, not just D+1.

    24-HE coverage gate applies in both modes. RT fills (region, date)
    tuples that lack forecast coverage; tuples beyond the RT mart's max
    date with no forecast coverage do not appear.

    Returns columns:
        as_of_date, date, hour_ending, region, source ('forecast'|'rt'),
        load_mw, solar_mw, wind_mw, net_load_mw,
        load_forecast_execution_datetime_local,
        solar_forecast_execution_datetime_local,
        wind_forecast_execution_datetime_local.
    ``as_of_date`` is NaT for RT rows.
    """
    if region != "RTO":
        raise ValueError(
            f"region={region!r} not supported; PJM net_load and solar/wind "
            "forecasts are RTO-only. Use load_load_coalesced for sub-zonal load."
        )

    # ── Forecast frame: pjm_net_load_forecast already aligns all 4 components ──
    fcst_full = load_pjm_net_load_forecast(cache_dir=cache_dir)
    fcst_full = fcst_full[fcst_full["region"].astype(str) == region].copy()
    if latest_only:
        fcst_full = _filter_to_latest_vintage_full_coverage(
            fcst_full, region_col="region"
        )
    elif lead_days is not None and "as_of_date" in fcst_full.columns:
        delta = (
            pd.to_datetime(fcst_full["date"], errors="coerce")
            - pd.to_datetime(fcst_full["as_of_date"], errors="coerce")
        ).dt.days
        fcst_full = fcst_full[delta == lead_days].copy()

    # A (region, date) is forecast-covered only when all 24 HEs are present.
    hour_counts = fcst_full.groupby(["region", "date"])["hour_ending"].nunique()
    covered_keys = hour_counts[hour_counts >= 24].index

    fcst_idx = pd.MultiIndex.from_arrays([fcst_full["region"], fcst_full["date"]])
    fcst_kept = fcst_full[fcst_idx.isin(covered_keys)].copy()
    fcst_kept = fcst_kept.rename(
        columns={
            "forecast_load_mw": "load_mw",
            "solar_forecast": "solar_mw",
            "wind_forecast": "wind_mw",
            "net_load_forecast_mw": "net_load_mw",
        }
    )
    fcst_kept["source"] = "forecast"
    if "as_of_date" not in fcst_kept.columns:
        fcst_kept["as_of_date"] = pd.NaT

    # ── RT frame: pjm_net_load_rt_hourly already exposes all 4 components ──
    rt_full = load_net_load_actuals(cache_dir=cache_dir)
    rt = (
        rt_full[rt_full["region"].astype(str) == region]
        .rename(
            columns={
                "rt_load_mw": "load_mw",
                "solar_gen_mw": "solar_mw",
                "wind_gen_mw": "wind_mw",
            }
        )
        .copy()
    )
    rt_idx = pd.MultiIndex.from_arrays([rt["region"], rt["date"]])
    rt_fallback = rt[~rt_idx.isin(covered_keys)].copy()
    rt_fallback["source"] = "rt"
    rt_fallback["as_of_date"] = pd.NaT
    for c in (
        "load_forecast_execution_datetime_local",
        "solar_forecast_execution_datetime_local",
        "wind_forecast_execution_datetime_local",
    ):
        rt_fallback[c] = pd.NaT

    out_cols = [
        "as_of_date",
        "date",
        "hour_ending",
        "region",
        "source",
        "load_mw",
        "solar_mw",
        "wind_mw",
        "net_load_mw",
        "load_forecast_execution_datetime_local",
        "solar_forecast_execution_datetime_local",
        "wind_forecast_execution_datetime_local",
    ]
    # Defensive: backfill any missing output column on either side with NaT/NaN.
    for c in out_cols:
        if c not in fcst_kept.columns:
            fcst_kept[c] = (
                pd.NaT if ("datetime" in c or c == "as_of_date") else float("nan")
            )
        if c not in rt_fallback.columns:
            rt_fallback[c] = (
                pd.NaT if ("datetime" in c or c == "as_of_date") else float("nan")
            )

    out = (
        pd.concat(
            [fcst_kept[out_cols], rt_fallback[out_cols]],
            ignore_index=True,
        )
        .sort_values(["region", "date", "hour_ending"])
        .reset_index(drop=True)
    )
    return _apply_column_filter(out, columns)


def load_meteologica_net_load_coalesced(
    *,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    lead_days: int | None = 1,
    latest_only: bool = False,
) -> pd.DataFrame:
    """Meteologica-first net-load — strict 24-HE coverage gate (4 regions).

    Superseded for new code by ``load_meteologica_supply_demand_coalesced``.
    Output is internally consistent on its own; do NOT compose
    ``net_load_mw`` from this function with values from
    ``load_meteologica_solar_coalesced`` / ``load_meteologica_wind_coalesced``
    — the per-series coalescers can disagree on forecast-vs-RT and break
    the identity ``net_load = load - solar - wind``.

    Mirrors ``load_meteologica_load_coalesced`` but for net-load (load minus
    reported solar + wind). Covers RTO + MIDATL/WEST/SOUTH sub-zones. PJM RT
    actuals (``net_load_actual``) fill (region, date) tuples without
    Meteologica forecast coverage.

    Two modes (mutually exclusive — ``latest_only=True`` ignores ``lead_days``):

      - ``lead_days=N`` (default ``lead_days=1``): pick the DA-cutoff
        vintage (``as_of_date == date - N``). ``None`` skips the filter.
      - ``latest_only=True``: pick the most-recent ``as_of_date`` per
        region and surface every forecast_date with full 24-HE coverage.

    24-HE coverage gate applies in both modes.

    Returns columns: as_of_date, date, hour_ending, region, net_load_mw,
    forecast_execution_datetime_local, source ('meteologica'|'rt').
    ``as_of_date`` is NaT for RT rows.
    """
    fcst_full = load_meteologica_net_load_forecast(cache_dir=cache_dir)
    if latest_only:
        fcst_full = _filter_to_latest_vintage_full_coverage(
            fcst_full, region_col="region"
        )
    elif lead_days is not None and "as_of_date" in fcst_full.columns:
        delta = (
            pd.to_datetime(fcst_full["date"], errors="coerce")
            - pd.to_datetime(fcst_full["as_of_date"], errors="coerce")
        ).dt.days
        fcst_full = fcst_full[delta == lead_days].copy()

    fcst_cols = [
        "date",
        "hour_ending",
        "region",
        "net_load_forecast_mw",
        "load_forecast_execution_datetime_local",
    ]
    if "as_of_date" in fcst_full.columns:
        fcst_cols = ["as_of_date"] + fcst_cols
    fcst = fcst_full[fcst_cols].rename(
        columns={
            "net_load_forecast_mw": "net_load_mw",
            "load_forecast_execution_datetime_local": "forecast_execution_datetime_local",
        }
    )
    if "as_of_date" not in fcst.columns:
        fcst["as_of_date"] = pd.NaT
    rt = (
        load_net_load_actuals(cache_dir=cache_dir)[
            ["date", "hour_ending", "region", "net_load_mw"]
        ]
        .dropna(subset=["net_load_mw"])
        .assign(forecast_execution_datetime_local=pd.NaT, as_of_date=pd.NaT)
    )

    hour_counts = fcst.groupby(["region", "date"])["hour_ending"].nunique()
    covered_keys = hour_counts[hour_counts >= 24].index

    fcst_idx = pd.MultiIndex.from_arrays([fcst["region"], fcst["date"]])
    fcst_kept = fcst[fcst_idx.isin(covered_keys)].assign(source="meteologica")

    rt_idx = pd.MultiIndex.from_arrays([rt["region"], rt["date"]])
    rt_fallback = rt[~rt_idx.isin(covered_keys)].assign(source="rt")

    out_cols = [
        "as_of_date",
        "date",
        "hour_ending",
        "region",
        "net_load_mw",
        "forecast_execution_datetime_local",
        "source",
    ]
    out = (
        pd.concat([fcst_kept[out_cols], rt_fallback[out_cols]], ignore_index=True)
        .sort_values(["region", "date", "hour_ending"])
        .reset_index(drop=True)
    )
    return _apply_column_filter(out, columns)


def load_meteologica_supply_demand_coalesced(
    *,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    lead_days: int | None = 1,
    latest_only: bool = False,
) -> pd.DataFrame:
    """Forecast-first hourly supply-demand bundle (Meteologica): load + solar + wind + net_load.

    Meteologica equivalent of ``load_pjm_supply_demand_coalesced``. Single
    source decision per (region, date) for ALL FOUR series simultaneously.
    A (region, date) is 'meteologica' iff the DA-cutoff Meteologica
    net_load forecast has full 24-hour coverage (which — by upstream
    construction in dbt's ``meteologica_pjm_net_load_forecast_hourly_da_cutoff``
    JOIN of the three component forecasts — implies load+solar+wind are
    all present). Otherwise the date falls back to PJM RT actuals for all
    four. Meteologica has no RT side; ``pjm_net_load_rt_hourly`` provides
    the unified-component RT fallback.

    Eliminates the cross-source mixing artifact where the per-series
    Meteologica coalescers (``load_meteologica_load_coalesced``,
    ``load_meteologica_solar_coalesced``, ``load_meteologica_wind_coalesced``,
    ``load_meteologica_net_load_coalesced``) could each independently
    decide forecast-vs-RT, breaking the identity
    ``net_load = load - solar - wind`` on dates with mixed coverage.
    Both source paths preserve the identity by construction:

      - Meteologica rows come from
        ``meteologica_pjm_net_load_forecast_hourly_da_cutoff``
        (built upstream by JOIN of the three component forecasts).
      - RT rows come from ``pjm_net_load_rt_hourly`` (built upstream by
        LEFT JOIN of the three component RT marts).

    Meteologica covers all 4 regions (RTO + MIDATL/WEST/SOUTH); PJM RT
    fallback also covers all 4.

    Two modes (mutually exclusive — ``latest_only=True`` ignores ``lead_days``):

      - ``lead_days=N`` (default ``lead_days=1``): pick the DA-cutoff
        vintage (``as_of_date == forecast_date - N``). ``None`` skips
        the filter.
      - ``latest_only=True``: pick the most-recent ``as_of_date`` per
        region and surface every forecast_date with full 24-HE coverage
        — exposes the full multi-day Meteologica horizon, not just D+1.

    24-HE coverage gate applies in both modes. RT fills (region, date)
    tuples that lack forecast coverage; tuples beyond the RT mart's max
    date with no forecast coverage do not appear.

    Returns columns:
        as_of_date, date, hour_ending, region, source ('meteologica'|'rt'),
        load_mw, solar_mw, wind_mw, net_load_mw,
        load_forecast_execution_datetime_local,
        solar_forecast_execution_datetime_local,
        wind_forecast_execution_datetime_local.
    ``as_of_date`` is NaT for RT rows.
    """
    # ── Forecast frame: meteologica_pjm_net_load_forecast aligns all 4 components ──
    fcst_full = load_meteologica_net_load_forecast(cache_dir=cache_dir)
    if latest_only:
        fcst_full = _filter_to_latest_vintage_full_coverage(
            fcst_full, region_col="region"
        )
    elif lead_days is not None and "as_of_date" in fcst_full.columns:
        delta = (
            pd.to_datetime(fcst_full["date"], errors="coerce")
            - pd.to_datetime(fcst_full["as_of_date"], errors="coerce")
        ).dt.days
        fcst_full = fcst_full[delta == lead_days].copy()

    # A (region, date) is forecast-covered only when all 24 HEs are present.
    hour_counts = fcst_full.groupby(["region", "date"])["hour_ending"].nunique()
    covered_keys = hour_counts[hour_counts >= 24].index

    fcst_idx = pd.MultiIndex.from_arrays([fcst_full["region"], fcst_full["date"]])
    fcst_kept = fcst_full[fcst_idx.isin(covered_keys)].copy()
    fcst_kept = fcst_kept.rename(
        columns={
            "forecast_load_mw": "load_mw",
            "solar_forecast": "solar_mw",
            "wind_forecast": "wind_mw",
            "net_load_forecast_mw": "net_load_mw",
        }
    )
    fcst_kept["source"] = "meteologica"
    if "as_of_date" not in fcst_kept.columns:
        fcst_kept["as_of_date"] = pd.NaT

    # ── RT frame: pjm_net_load_rt_hourly already exposes all 4 components ──
    rt_full = load_net_load_actuals(cache_dir=cache_dir)
    rt = rt_full.rename(
        columns={
            "rt_load_mw": "load_mw",
            "solar_gen_mw": "solar_mw",
            "wind_gen_mw": "wind_mw",
        }
    ).copy()
    rt_idx = pd.MultiIndex.from_arrays([rt["region"], rt["date"]])
    rt_fallback = rt[~rt_idx.isin(covered_keys)].copy()
    rt_fallback["source"] = "rt"
    rt_fallback["as_of_date"] = pd.NaT
    for c in (
        "load_forecast_execution_datetime_local",
        "solar_forecast_execution_datetime_local",
        "wind_forecast_execution_datetime_local",
    ):
        rt_fallback[c] = pd.NaT

    out_cols = [
        "as_of_date",
        "date",
        "hour_ending",
        "region",
        "source",
        "load_mw",
        "solar_mw",
        "wind_mw",
        "net_load_mw",
        "load_forecast_execution_datetime_local",
        "solar_forecast_execution_datetime_local",
        "wind_forecast_execution_datetime_local",
    ]
    # Defensive: backfill any missing output column on either side with NaT/NaN.
    for c in out_cols:
        if c not in fcst_kept.columns:
            fcst_kept[c] = (
                pd.NaT if ("datetime" in c or c == "as_of_date") else float("nan")
            )
        if c not in rt_fallback.columns:
            rt_fallback[c] = (
                pd.NaT if ("datetime" in c or c == "as_of_date") else float("nan")
            )

    out = (
        pd.concat(
            [fcst_kept[out_cols], rt_fallback[out_cols]],
            ignore_index=True,
        )
        .sort_values(["region", "date", "hour_ending"])
        .reset_index(drop=True)
    )
    return _apply_column_filter(out, columns)


def load_installed_capacity(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Energy Aspects monthly PJM installed capacity, fuel-disaggregated.

    Forward-projected through ~2030. Returns one row per month with
    canonical fuel column names plus total_installed_capacity_mw.
    """
    return _load_dataset(
        "installed_capacity", path=path, cache_dir=cache_dir, columns=columns
    )


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
    return _load_dataset(
        "day_gen_capacity", path=path, cache_dir=cache_dir, columns=columns
    )


def load_pjm_dates_daily(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """PJM calendar metadata, one row per delivery date.

    Columns: date, day_of_week_number (Sun=0..Sat=6), is_weekend,
    is_nerc_holiday, is_federal_holiday, summer_winter, holiday_name.
    Sorted by date, deduplicated (last write wins per date).
    """
    return _load_dataset(
        "pjm_dates_daily", path=path, cache_dir=cache_dir, columns=columns
    )


def load_reserve_market_results_hourly(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """PJM cleared operating-reserve market, hourly, system-wide (PJM_RTO).

    Wide per (date, hour_ending) with per-service columns (SR / PR / 30MIN
    / REG) for cleared MW, requirement MW, and MCP, plus the two derived
    columns the supply-stack model reads -- ``operating_reserve_mw_cleared``
    (SR + PR + 30MIN total_mw) and ``operating_reserve_requirement_mw``
    (SR + PR + 30MIN as_req_mw, the more stable forward proxy) -- and the
    ``reserve_scarcity_flag`` boolean (MCP > $10 in any of SR/PR/30MIN).

    Backward-only feed: today and forward delivery dates won't have rows.
    Forward-looking consumers compute a rolling profile by (DOW, HE) from
    the historical rows. Mart: ``pjm_da_modelling_cleaned.pjm_reserve_market_results_hourly``.
    """
    return _load_dataset(
        "reserve_market_results_hourly", path=path, cache_dir=cache_dir, columns=columns
    )


def load_weather_hourly(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "weather_hourly", path=path, cache_dir=cache_dir, columns=columns
    )


def load_weather_observed_hourly(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "weather_observed_hourly", path=path, cache_dir=cache_dir, columns=columns
    )


def load_weather_forecast_hourly(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "weather_forecast_hourly", path=path, cache_dir=cache_dir, columns=columns
    )


def load_weather_coalesced(
    *,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Observed-first hourly RTO weather — the unified series models should consume.

    For each date, the WSI observed record wins when the historical parquet
    has all 24 hour_ending values; the latest forecast vintage fills every
    other date — including future dates and any partial-coverage gaps in
    history. Mirrors the strict 24-HE rule in ``load_solar_coalesced`` /
    ``load_load_coalesced`` so daily aggregations never compose observed
    and forecast hours within the same date.

    Both parquets are RTO-wide (no region column on either side), so the
    output carries no region column.

    Forecast priority is reversed relative to the load / solar / wind
    coalescers: weather observed actuals ARE the ground truth (vs. RT
    actuals which lag the day-ahead decision), so observed-first is the
    natural rule. Live-forecast query code that needs forecast-first
    behavior for a future ``target_date`` should keep calling
    ``load_weather_forecast_hourly`` directly.

    ``relative_humidity`` exists on observed but not on the forecast
    parquet, so it's dropped from the coalesced output (matches the
    "carry only cols both sides have" convention used by the other
    coalescers).

    Returns columns: date, hour_ending, temp, feels_like_temp,
    dew_point_temp, wind_speed_mph, cloud_cover_pct,
    source ('observed'|'forecast').
    """
    keep_cols = [
        "date",
        "hour_ending",
        "temp",
        "feels_like_temp",
        "dew_point_temp",
        "wind_speed_mph",
        "cloud_cover_pct",
    ]
    obs_full = load_weather_observed_hourly(cache_dir=cache_dir)
    obs = obs_full[[c for c in keep_cols if c in obs_full.columns]].copy()

    fcst_full = load_weather_forecast_hourly(cache_dir=cache_dir)
    fcst = fcst_full[[c for c in keep_cols if c in fcst_full.columns]].copy()

    # A date is "observed-covered" only when all 24 HEs are present.
    hour_counts = obs.groupby("date")["hour_ending"].nunique()
    covered_dates = hour_counts[hour_counts >= 24].index

    obs_kept = obs[obs["date"].isin(covered_dates)].assign(source="observed")
    fcst_fallback = fcst[~fcst["date"].isin(covered_dates)].assign(source="forecast")

    out = (
        pd.concat([obs_kept, fcst_fallback], ignore_index=True)
        .sort_values(["date", "hour_ending"])
        .reset_index(drop=True)
    )
    return _apply_column_filter(out, columns)


def load_gas_prices_hourly(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "gas_prices_hourly", path=path, cache_dir=cache_dir, columns=columns
    )


def load_meteologica_load_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "meteologica_load_forecast",
        path=path,
        cache_dir=cache_dir,
        columns=columns,
    )


def load_meteologica_load_coalesced(
    *,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    lead_days: int | None = 1,
    latest_only: bool = False,
) -> pd.DataFrame:
    """Meteologica-first hourly load — strict 24-HE coverage gate.

    Mirrors ``load_load_coalesced`` but uses Meteologica as the forecast
    source instead of PJM-native. PJM RT actuals fill (region, date)
    tuples without Meteologica forecast coverage.

    Two modes (mutually exclusive — ``latest_only=True`` ignores ``lead_days``):

      - ``lead_days=N`` (default ``lead_days=1``): pick the DA-cutoff
        vintage (``as_of_date == date - N``). ``None`` skips the filter.
      - ``latest_only=True``: pick the most-recent ``as_of_date`` per
        region and surface every forecast_date with full 24-HE coverage.

    24-HE coverage gate applies in both modes.

    Designed as an alt-source signal for the model. Streamlit consumes
    this for visualization; downstream model wiring is opt-in.

    Returns columns: as_of_date, date, hour_ending, region, load_mw,
    forecast_execution_datetime_local, source ('meteologica'|'rt').
    ``as_of_date`` is NaT for RT rows.
    """
    fcst_full = load_meteologica_load_forecast(cache_dir=cache_dir)
    if latest_only:
        fcst_full = _filter_to_latest_vintage_full_coverage(
            fcst_full, region_col="region"
        )
    elif lead_days is not None and "as_of_date" in fcst_full.columns:
        delta = (
            pd.to_datetime(fcst_full["date"], errors="coerce")
            - pd.to_datetime(fcst_full["as_of_date"], errors="coerce")
        ).dt.days
        fcst_full = fcst_full[delta == lead_days].copy()

    fcst_cols = [
        "date",
        "hour_ending",
        "region",
        "forecast_load_mw",
        "forecast_execution_datetime_local",
    ]
    if "as_of_date" in fcst_full.columns:
        fcst_cols = ["as_of_date"] + fcst_cols
    fcst = fcst_full[fcst_cols].rename(columns={"forecast_load_mw": "load_mw"})
    if "as_of_date" not in fcst.columns:
        fcst["as_of_date"] = pd.NaT
    rt = (
        load_load_rt(cache_dir=cache_dir)[
            ["date", "hour_ending", "region", "rt_load_mw"]
        ]
        .rename(columns={"rt_load_mw": "load_mw"})
        .assign(forecast_execution_datetime_local=pd.NaT, as_of_date=pd.NaT)
    )

    # A (region, date) is "Meteologica-covered" only when all 24 HEs are present.
    hour_counts = fcst.groupby(["region", "date"])["hour_ending"].nunique()
    covered_keys = hour_counts[hour_counts >= 24].index

    fcst_idx = pd.MultiIndex.from_arrays([fcst["region"], fcst["date"]])
    fcst_kept = fcst[fcst_idx.isin(covered_keys)].assign(source="meteologica")

    rt_idx = pd.MultiIndex.from_arrays([rt["region"], rt["date"]])
    rt_fallback = rt[~rt_idx.isin(covered_keys)].assign(source="rt")

    out_cols = [
        "as_of_date",
        "date",
        "hour_ending",
        "region",
        "load_mw",
        "forecast_execution_datetime_local",
        "source",
    ]
    out = (
        pd.concat([fcst_kept[out_cols], rt_fallback[out_cols]], ignore_index=True)
        .sort_values(["region", "date", "hour_ending"])
        .reset_index(drop=True)
    )
    return _apply_column_filter(out, columns)


def load_meteologica_solar_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "meteologica_solar_forecast",
        path=path,
        cache_dir=cache_dir,
        columns=columns,
    )


def load_meteologica_solar_coalesced(
    *,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    lead_days: int | None = 1,
    latest_only: bool = False,
) -> pd.DataFrame:
    """Meteologica-first hourly solar — strict 24-HE coverage gate.

    Mirrors ``load_meteologica_load_coalesced`` but uses Meteologica solar
    as the forecast source and PJM ``solar_gen_mw`` actuals as the
    fallback.

    Two modes (mutually exclusive — ``latest_only=True`` ignores ``lead_days``):

      - ``lead_days=N`` (default ``lead_days=1``): pick the DA-cutoff
        vintage (``as_of_date == date - N``). ``None`` skips the filter.
      - ``latest_only=True``: pick the most-recent ``as_of_date`` per
        region and surface every forecast_date with full 24-HE coverage.

    24-HE coverage gate applies in both modes.

    PJM did not publish solar generation actuals before 2019-04-02, so
    rows with NaN ``solar_gen_mw`` are dropped. As a result the RT
    fallback portion of the output starts at ~2019-04-02 — pre-2019-04-02
    dates simply have no data on either side.

    Returns columns: as_of_date, date, hour_ending, region, solar_mw,
    forecast_execution_datetime_local, source ('meteologica'|'rt').
    ``as_of_date`` is NaT for RT rows.
    """
    fcst_full = load_meteologica_solar_forecast(cache_dir=cache_dir)
    if latest_only:
        fcst_full = _filter_to_latest_vintage_full_coverage(
            fcst_full, region_col="region"
        )
    elif lead_days is not None and "as_of_date" in fcst_full.columns:
        delta = (
            pd.to_datetime(fcst_full["date"], errors="coerce")
            - pd.to_datetime(fcst_full["as_of_date"], errors="coerce")
        ).dt.days
        fcst_full = fcst_full[delta == lead_days].copy()

    fcst_cols = [
        "date",
        "hour_ending",
        "region",
        "solar_forecast",
        "forecast_execution_datetime_local",
    ]
    if "as_of_date" in fcst_full.columns:
        fcst_cols = ["as_of_date"] + fcst_cols
    fcst = fcst_full[fcst_cols].rename(columns={"solar_forecast": "solar_mw"})
    if "as_of_date" not in fcst.columns:
        fcst["as_of_date"] = pd.NaT
    rt = (
        load_net_load_actuals(cache_dir=cache_dir)[
            ["date", "hour_ending", "region", "solar_gen_mw"]
        ]
        .rename(columns={"solar_gen_mw": "solar_mw"})
        .dropna(subset=["solar_mw"])
        .assign(forecast_execution_datetime_local=pd.NaT, as_of_date=pd.NaT)
    )

    # A (region, date) is "Meteologica-covered" only when all 24 HEs are present.
    hour_counts = fcst.groupby(["region", "date"])["hour_ending"].nunique()
    covered_keys = hour_counts[hour_counts >= 24].index

    fcst_idx = pd.MultiIndex.from_arrays([fcst["region"], fcst["date"]])
    fcst_kept = fcst[fcst_idx.isin(covered_keys)].assign(source="meteologica")

    rt_idx = pd.MultiIndex.from_arrays([rt["region"], rt["date"]])
    rt_fallback = rt[~rt_idx.isin(covered_keys)].assign(source="rt")

    out_cols = [
        "as_of_date",
        "date",
        "hour_ending",
        "region",
        "solar_mw",
        "forecast_execution_datetime_local",
        "source",
    ]
    out = (
        pd.concat([fcst_kept[out_cols], rt_fallback[out_cols]], ignore_index=True)
        .sort_values(["region", "date", "hour_ending"])
        .reset_index(drop=True)
    )
    return _apply_column_filter(out, columns)


def load_meteologica_wind_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "meteologica_wind_forecast",
        path=path,
        cache_dir=cache_dir,
        columns=columns,
    )


def load_meteologica_wind_coalesced(
    *,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    lead_days: int | None = 1,
    latest_only: bool = False,
) -> pd.DataFrame:
    """Meteologica-first hourly wind — strict 24-HE coverage gate.

    Mirrors ``load_meteologica_load_coalesced`` but uses Meteologica wind
    as the forecast source and PJM ``wind_gen_mw`` actuals as the fallback.

    Unlike ``load_wind_coalesced`` (RTO-only, since the PJM-native wind
    forecast is system-wide), this loader is regional — Meteologica
    publishes wind per region (RTO/MIDATL/WEST/SOUTH).

    Two modes (mutually exclusive — ``latest_only=True`` ignores ``lead_days``):

      - ``lead_days=N`` (default ``lead_days=1``): pick the DA-cutoff
        vintage (``as_of_date == date - N``). ``None`` skips the filter.
      - ``latest_only=True``: pick the most-recent ``as_of_date`` per
        region and surface every forecast_date with full 24-HE coverage.

    24-HE coverage gate applies in both modes. Some older dates have
    NaN ``wind_gen_mw`` actuals; those rows are dropped from the RT
    fallback so the output never carries NaN values.

    Returns columns: as_of_date, date, hour_ending, region, wind_mw,
    forecast_execution_datetime_local, source ('meteologica'|'rt').
    ``as_of_date`` is NaT for RT rows.
    """
    fcst_full = load_meteologica_wind_forecast(cache_dir=cache_dir)
    if latest_only:
        fcst_full = _filter_to_latest_vintage_full_coverage(
            fcst_full, region_col="region"
        )
    elif lead_days is not None and "as_of_date" in fcst_full.columns:
        delta = (
            pd.to_datetime(fcst_full["date"], errors="coerce")
            - pd.to_datetime(fcst_full["as_of_date"], errors="coerce")
        ).dt.days
        fcst_full = fcst_full[delta == lead_days].copy()

    fcst_cols = [
        "date",
        "hour_ending",
        "region",
        "wind_forecast",
        "forecast_execution_datetime_local",
    ]
    if "as_of_date" in fcst_full.columns:
        fcst_cols = ["as_of_date"] + fcst_cols
    fcst = fcst_full[fcst_cols].rename(columns={"wind_forecast": "wind_mw"})
    if "as_of_date" not in fcst.columns:
        fcst["as_of_date"] = pd.NaT
    rt = (
        load_net_load_actuals(cache_dir=cache_dir)[
            ["date", "hour_ending", "region", "wind_gen_mw"]
        ]
        .rename(columns={"wind_gen_mw": "wind_mw"})
        .dropna(subset=["wind_mw"])
        .assign(forecast_execution_datetime_local=pd.NaT, as_of_date=pd.NaT)
    )

    # A (region, date) is "Meteologica-covered" only when all 24 HEs are present.
    hour_counts = fcst.groupby(["region", "date"])["hour_ending"].nunique()
    covered_keys = hour_counts[hour_counts >= 24].index

    fcst_idx = pd.MultiIndex.from_arrays([fcst["region"], fcst["date"]])
    fcst_kept = fcst[fcst_idx.isin(covered_keys)].assign(source="meteologica")

    rt_idx = pd.MultiIndex.from_arrays([rt["region"], rt["date"]])
    rt_fallback = rt[~rt_idx.isin(covered_keys)].assign(source="rt")

    out_cols = [
        "as_of_date",
        "date",
        "hour_ending",
        "region",
        "wind_mw",
        "forecast_execution_datetime_local",
        "source",
    ]
    out = (
        pd.concat([fcst_kept[out_cols], rt_fallback[out_cols]], ignore_index=True)
        .sort_values(["region", "date", "hour_ending"])
        .reset_index(drop=True)
    )
    return _apply_column_filter(out, columns)


def load_meteologica_net_load_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    return _load_dataset(
        "meteologica_net_load_forecast",
        path=path,
        cache_dir=cache_dir,
        columns=columns,
    )


def load_meteologica_da_price_forecast(
    *,
    path: str | Path | None = None,
    cache_dir: str | Path | None = None,
    columns: Iterable[str] | None = None,
    lead_days: int | None = 1,
    latest_only: bool = False,
) -> pd.DataFrame:
    """Meteologica PJM Western-Hub DA price forecast (deterministic + ENS summary).

    Single price node (Western Hub) — no region dim. The historical mart
    carries multiple vintages per (forecast_date, hour_ending) keyed by
    ``as_of_date``.

    Two modes (mutually exclusive — ``latest_only=True`` ignores ``lead_days``):

      - ``lead_days=N`` (default ``lead_days=1``): pick the DA-cutoff
        vintage (``as_of_date == forecast_date - N``). ``None`` skips
        the filter.
      - ``latest_only=True``: pick the single most-recent ``as_of_date``
        (system-wide — no region) and drop forecast_dates without all
        24 hour_ending values under that vintage. Lets callers consume
        the full multi-day Meteologica price horizon.

    Returns columns: as_of_date, date, hour_ending,
    da_price_deterministic, da_price_ens_average, da_price_ens_bottom,
    da_price_ens_top, det_forecast_execution_datetime_local,
    ens_forecast_execution_datetime_local, da_price_ens_00, ...,
    da_price_ens_50 (the 51 ECMWF members, when present in the parquet).
    """
    df = _load_dataset(
        "meteologica_da_price_forecast",
        path=path,
        cache_dir=cache_dir,
        columns=None,
    )
    if latest_only:
        df = _filter_to_latest_vintage_full_coverage(df, region_col=None)
    elif lead_days is not None and "as_of_date" in df.columns:
        delta = (
            pd.to_datetime(df["date"], errors="coerce")
            - pd.to_datetime(df["as_of_date"], errors="coerce")
        ).dt.days
        df = df[delta == lead_days].copy()
    return _apply_column_filter(df, columns)
