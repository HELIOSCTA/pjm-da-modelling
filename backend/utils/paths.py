"""Path registry for backend report bundles.

Single source of truth for `backend/cache/` parquet locations and bundle paths.
Bundles avoid per-folder BASE_DIR boilerplate by calling:

    from backend.utils import paths
    paths.parquet("fuel_mix")              # -> backend/cache/pjm_fuel_mix_hourly.parquet
    paths.bundle_sql(__file__, "name.sql") # -> <bundle>/sql/name.sql

Adding a new dataset = one PARQUETS entry + the bundle directory.
"""
from __future__ import annotations

from pathlib import Path

from backend.settings import BASE_DIR, CACHE_DIR

BACKEND_ROOT = BASE_DIR
REPORTS_ROOT = BACKEND_ROOT / "reports"
OUTPUT_DIR = REPORTS_ROOT / "_output"

PARQUETS: dict[str, str] = {
    "lmps_hourly":                   "pjm_lmps_hourly.parquet",
    "fuel_mix":                      "pjm_fuel_mix_hourly.parquet",
    "outages_actual":                "pjm_outages_actual_daily.parquet",
    "outages_forecast":              "pjm_outages_forecast_history.parquet",
    "load_forecast":                 "pjm_load_forecast_hourly_da_cutoff_historical.parquet",
    "solar_forecast":                "pjm_solar_forecast_hourly_da_cutoff_historical.parquet",
    "wind_forecast":                 "pjm_wind_forecast_hourly_da_cutoff_historical.parquet",
    "meteologica_load_forecast":     "meteologica_pjm_load_forecast_hourly_da_cutoff_historical.parquet",
    "meteologica_solar_forecast":    "meteologica_pjm_solar_forecast_hourly_da_cutoff_historical.parquet",
    "meteologica_wind_forecast":     "meteologica_pjm_wind_forecast_hourly_da_cutoff_historical.parquet",
    "meteologica_net_load_forecast": "meteologica_pjm_net_load_forecast_hourly_da_cutoff_historical.parquet",
    "psse_buses":                    "psse_buses.parquet",
    "psse_branches":                 "psse_branches.parquet",
}


def parquet(key: str) -> Path:
    """Resolve a dataset key to its parquet path under backend/cache/."""
    if key not in PARQUETS:
        raise KeyError(f"Unknown dataset key: {key!r}. Known keys: {sorted(PARQUETS)}")
    return CACHE_DIR / PARQUETS[key]


def bundle_sql(bundle_file: str | Path, sql_filename: str) -> Path:
    """Resolve a bundle's SQL file. Pass __file__ from the bundle module."""
    return Path(bundle_file).resolve().parent / "sql" / sql_filename
