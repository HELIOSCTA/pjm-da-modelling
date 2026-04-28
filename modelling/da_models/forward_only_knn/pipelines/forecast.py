"""Single-day forward-only KNN forecast pipeline."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.common.data import loader
from da_models.common.data.loader import _existing_candidates, _resolve_cache_dir
from da_models.forward_only_knn import configs
from da_models.forward_only_knn.features.builder import build_pool, build_query_row
from da_models.forward_only_knn.similarity.engine import find_twins
from da_models.forward_only_knn.validation.preflight import run_preflight
from utils.logging_utils import (
    Colors,
    get_logger,
    print_divider,
    print_header,
    print_section,
    supports_color,
)

# Datasets shown in the input-feed provenance block. Each tuple is
# (label, dataset_key, fallback_dataset_key_or_None, gated_by_flag).
_FEED_SOURCES: list[tuple[str, str, str | None, str | None]] = [
    ("Load RTO",      "load_forecast",                    "load_rt", None),
    ("Load Meteo",    "meteologica_load_forecast",        None,      None),
    ("Gas",           "gas_prices_hourly",                None,      "include_gas"),
    ("Outages",       "outages_forecast",                 None,      "include_outages"),
    ("Solar RTO",     "solar_forecast",                   None,      "include_renewables"),
    ("Solar Meteo",   "meteologica_solar_forecast",       None,      "include_renewables"),
    ("Wind RTO",      "wind_forecast",                    None,      "include_renewables"),
    ("Wind Meteo",    "meteologica_wind_forecast",        None,      "include_renewables"),
    ("NetLoad RTO",   "net_load_forecast",                None,      "include_net_load"),
    ("NetLoad Meteo", "meteologica_net_load_forecast",    None,      "include_net_load"),
]

# Hub-column labels for hourly gas prices in the query feature table.
_GAS_HUB_LABELS: dict[str, str] = {
    "gas_m3": "TETCO M3",
    "gas_tco": "COL TCO",
    "gas_tz6": "TRANSCO Z6",
    "gas_dom_south": "DOM SOUTH",
}

logger = get_logger()

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]
DAY_ABBR = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}

_HL_FORECAST = (Colors.BOLD + Colors.BRIGHT_RED) if supports_color() else ""
_HL_QUARTILE = Colors.BRIGHT_CYAN if supports_color() else ""
_HL_INNER = Colors.BRIGHT_YELLOW if supports_color() else ""
_HL_ACTUAL = Colors.BRIGHT_GREEN if supports_color() else ""
_HL_ERROR = Colors.DIM if supports_color() else ""
_RS = Colors.RESET if supports_color() else ""

_ROW_STYLES = {
    "Actual": _HL_ACTUAL,
    "Forecast": _HL_FORECAST,
    "Error": _HL_ERROR,
    "P25": _HL_QUARTILE, "P75": _HL_QUARTILE,
    "P37.5": _HL_INNER, "P62.5": _HL_INNER,
}

# Per-analog accent colors (cycled through top-5).
_ANALOG_PALETTE: list[str] = (
    [
        Colors.BRIGHT_CYAN,
        Colors.BRIGHT_YELLOW,
        Colors.BRIGHT_GREEN,
        Colors.BRIGHT_MAGENTA,
        Colors.BRIGHT_BLUE,
    ]
    if supports_color()
    else ["", "", "", "", ""]
)


def weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    """Weighted quantile via cumulative interpolation."""
    idx = np.argsort(values)
    v = values[idx]
    w = weights[idx]
    cdf = np.cumsum(w)
    cdf = cdf / cdf[-1]
    return float(np.interp(q, cdf, v))


def _quantile_label(q: float) -> str:
    q_pct = q * 100.0
    if float(q_pct).is_integer():
        return f"P{int(q_pct):02d}"
    return f"P{q_pct:.1f}".rstrip("0").rstrip(".")


def _add_summary_cols(row_dict: dict) -> dict:
    on_vals = [row_dict.get(f"HE{h}") for h in ONPEAK_HOURS]
    off_vals = [row_dict.get(f"HE{h}") for h in OFFPEAK_HOURS]
    flat_vals = [row_dict.get(f"HE{h}") for h in configs.HOURS]

    on_vals = [v for v in on_vals if v is not None and not pd.isna(v)]
    off_vals = [v for v in off_vals if v is not None and not pd.isna(v)]
    flat_vals = [v for v in flat_vals if v is not None and not pd.isna(v)]

    row_dict["OnPeak"] = float(np.mean(on_vals)) if on_vals else np.nan
    row_dict["OffPeak"] = float(np.mean(off_vals)) if off_vals else np.nan
    row_dict["Flat"] = float(np.mean(flat_vals)) if flat_vals else np.nan
    return row_dict


def _build_output_table(
    target_date: date,
    forecast_hourly: dict[int, float],
    actual_hourly: dict[int, float] | None,
) -> pd.DataFrame:
    rows: list[dict] = []

    if actual_hourly is not None:
        actual_row = {"Date": target_date, "Type": "Actual"}
        for h in configs.HOURS:
            actual_row[f"HE{h}"] = actual_hourly.get(h)
        rows.append(_add_summary_cols(actual_row))

    forecast_row = {"Date": target_date, "Type": "Forecast"}
    for h in configs.HOURS:
        forecast_row[f"HE{h}"] = forecast_hourly.get(h)
    rows.append(_add_summary_cols(forecast_row))

    if actual_hourly is not None:
        error_row = {"Date": target_date, "Type": "Error"}
        for h in configs.HOURS:
            a = actual_hourly.get(h)
            f = forecast_hourly.get(h)
            error_row[f"HE{h}"] = (f - a) if (a is not None and f is not None) else None
        rows.append(_add_summary_cols(error_row))

    cols = ["Date", "Type"] + [f"HE{h}" for h in configs.HOURS] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def _actuals_from_pool(pool: pd.DataFrame, target_date: date) -> dict[int, float] | None:
    row = pool[pool["date"] == target_date]
    if len(row) == 0:
        return None
    rec = row.iloc[0]
    actuals: dict[int, float] = {}
    for h in configs.HOURS:
        val = rec.get(f"lmp_h{h}")
        if val is None or pd.isna(val):
            return None
        actuals[h] = float(val)
    return actuals


def _season_window_filter_for_preflight(
    pool: pd.DataFrame,
    target_date: date,
    season_window_days: int,
) -> pd.DataFrame:
    """Mirror season-window filtering used by analog selection for coverage checks."""
    hist = pool[pd.to_datetime(pool["date"]).dt.date < target_date].copy()
    if season_window_days <= 0 or len(hist) == 0:
        return hist

    target_doy = pd.Timestamp(target_date).dayofyear
    day_of_year = pd.to_datetime(hist["date"]).dt.dayofyear.to_numpy(dtype=float)
    direct = np.abs(day_of_year - float(target_doy))
    circular = np.minimum(direct, 366.0 - direct)
    return hist[circular <= float(season_window_days)].copy()


def _derive_effective_weights(
    base_weights: dict[str, float],
    missing_query_groups: list[str],
    low_pool_groups: list[str],
) -> tuple[dict[str, float], list[str]]:
    """Zero weak groups based on preflight coverage checks."""
    effective = dict(base_weights)
    disabled = sorted(set(missing_query_groups) | set(low_pool_groups))
    for group in disabled:
        if group in effective:
            effective[group] = 0.0

    if not any(float(weight) > 0 for weight in effective.values()):
        effective["calendar_dow"] = max(float(base_weights.get("calendar_dow", 0.0)), 1.0)
        disabled = sorted(set(disabled) - {"calendar_dow"})

    return effective, disabled


def _hourly_forecast_from_analogs(
    analogs: pd.DataFrame,
    quantiles: list[float],
) -> pd.DataFrame:
    rows: list[dict] = []
    for h in configs.HOURS:
        col = f"lmp_h{h}"
        if col not in analogs.columns:
            continue
        hour = analogs[["weight", col]].dropna(subset=[col]).copy()
        if len(hour) == 0:
            continue
        values = hour[col].to_numpy(dtype=float)
        weights = hour["weight"].to_numpy(dtype=float)
        weights = weights / weights.sum()

        row = {"hour_ending": h, "point_forecast": float(np.average(values, weights=weights))}
        for q in quantiles:
            row[f"q_{q:.2f}"] = weighted_quantile(values, weights, q)
        rows.append(row)
    return pd.DataFrame(rows)


def _validate_query_features(
    query: pd.Series,
    feature_weights: dict[str, float],
) -> dict[str, dict]:
    """Validate per-group feature coverage on the query row.

    Returns a dict {group: {present, missing, coverage}} and emits warnings
    for any active group with missing/NaN feature values.
    """
    report: dict[str, dict] = {}
    for group, cols in configs.FEATURE_GROUPS.items():
        weight = float(feature_weights.get(group, 0.0))
        present, missing = [], []
        for col in cols:
            val = query.get(col) if col in query.index else None
            if val is None or pd.isna(val):
                missing.append(col)
            else:
                present.append(col)
        coverage = len(present) / len(cols) if cols else 1.0
        report[group] = {
            "weight": weight,
            "present": present,
            "missing": missing,
            "coverage": coverage,
        }
        if weight > 0 and missing:
            logger.warning(
                "Query feature gap — group=%s coverage=%.0f%% missing=%s",
                group, coverage * 100, missing,
            )
    return report


def _validate_pool(pool: pd.DataFrame, target_date: date) -> None:
    """Validate the historical pool meets minimum quality bars."""
    if len(pool) == 0:
        logger.error("Historical pool is empty — cannot run forecast")
        return

    if "date" not in pool.columns:
        logger.error("Pool is missing 'date' column")
        return

    pool_dates = pd.to_datetime(pool["date"]).dt.date
    n_total = len(pool)
    n_history = int((pool_dates < target_date).sum())
    earliest = pool_dates.min()
    latest = pool_dates.max()

    logger.info(
        "Pool ready — rows=%d history_rows=%d range=%s..%s",
        n_total, n_history, earliest, latest,
    )

    lmp_cols = [f"lmp_h{h}" for h in configs.HOURS]
    missing_lmp_cols = [c for c in lmp_cols if c not in pool.columns]
    if missing_lmp_cols:
        logger.error("Pool missing hourly LMP columns: %s", missing_lmp_cols)
        return

    present_lmp_cols = [c for c in lmp_cols if c in pool.columns]
    nan_share = float(pool[present_lmp_cols].isna().to_numpy().mean())
    if nan_share > 0.05:
        logger.warning("Pool LMP NaN share is %.1f%% (>5%%)", nan_share * 100)

    if n_history < configs.MIN_POOL_SIZE:
        logger.warning(
            "History rows below MIN_POOL_SIZE — have=%d need=%d",
            n_history, configs.MIN_POOL_SIZE,
        )


def _validate_hourly_forecast(df_forecast: pd.DataFrame) -> None:
    """Validate the produced hourly forecast covers all 24 hours and is finite."""
    if "hour_ending" not in df_forecast.columns or "point_forecast" not in df_forecast.columns:
        logger.error("Hourly forecast missing expected columns")
        return

    covered = set(df_forecast["hour_ending"].astype(int).tolist())
    expected = set(configs.HOURS)
    gaps = sorted(expected - covered)
    if gaps:
        logger.error("Hourly forecast has missing hours: %s", gaps)

    pf = df_forecast["point_forecast"].to_numpy(dtype=float)
    n_nan = int(np.isnan(pf).sum())
    if n_nan:
        logger.error("Hourly forecast has %d NaN point values", n_nan)
    elif np.any(pf < -200) or np.any(pf > 2000):
        logger.warning(
            "Hourly forecast has out-of-range values — min=%.2f max=%.2f",
            float(pf.min()), float(pf.max()),
        )


def _resolve_source_path(cache_dir: Path | None, dataset_key: str) -> Path | None:
    """Return the parquet path the loader would actually pick for a dataset key."""
    candidates = _existing_candidates(_resolve_cache_dir(cache_dir), dataset_key)
    return candidates[0] if candidates else None


def _format_mtime(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    except OSError:
        return "?"


def _format_size_kb(path: Path) -> str:
    try:
        if path.is_dir():
            total = sum(p.stat().st_size for p in path.rglob("*") if p.is_file())
        else:
            total = path.stat().st_size
        return f"{total / 1024:,.0f} KB"
    except OSError:
        return "?"


def _print_forecast_sources(
    cache_dir: Path | None,
    target_date: date,
    flags: dict[str, bool],
) -> None:
    """Print the parquet file actually picked for each forward-looking feed."""
    print_header(
        f"INPUT FEED PROVENANCE  |  Target: {_format_target_label(target_date)}",
        length=90,
    )

    print(f"\n  {'Feed':<10} {'Picked file':<55} {'Size':>10}  {'Modified':<16}")
    print("  " + "-" * 96)

    cache_resolved = _resolve_cache_dir(cache_dir)
    for label, key, fallback_key, gate in _FEED_SOURCES:
        if gate is not None and not flags.get(gate, True):
            print(f"  {label:<10} (skipped — {gate}=False)")
            continue

        primary = _resolve_source_path(cache_dir, key)
        fallback = _resolve_source_path(cache_dir, fallback_key) if fallback_key else None
        picked = primary or fallback

        if picked is None:
            line = f"  {label:<10} {'<missing — no parquet found>':<55}"
            if supports_color():
                line = f"{Colors.BRIGHT_RED}{line}{Colors.RESET}"
            print(line)
            logger.error("Feed missing — %s key=%s cache=%s", label, key, cache_resolved)
            continue

        used_fallback = primary is None and fallback is not None
        marker = " (fallback)" if used_fallback else ""
        name = picked.name + marker
        line = f"  {label:<10} {name:<55} {_format_size_kb(picked):>10}  {_format_mtime(picked):<16}"
        if used_fallback and supports_color():
            line = f"{Colors.BRIGHT_YELLOW}{line}{Colors.RESET}"
        print(line)
        logger.info(
            "Feed source — %s -> %s (mtime=%s, fallback=%s)",
            label, picked.name, _format_mtime(picked), used_fallback,
        )

    print("\n  Cache dir: " + str(cache_resolved))
    print_divider("=", 90, dim=False)


def _safe_load_for_print(load_fn, cache_dir: Path | None) -> pd.DataFrame | None:
    """Cheap re-load for the print block; tolerate any loader failure."""
    try:
        return load_fn(cache_dir=cache_dir)
    except Exception as exc:
        logger.warning("Print loader failed for %s: %s", load_fn.__name__, exc)
        return None


def _format_query_cell(value: float | None, fmt: str) -> str:
    """Format one cell of the hourly query-feature table."""
    if value is None or pd.isna(value):
        return "-"
    if fmt == "comma":
        return f"{float(value):,.0f}"
    if fmt == "1f":
        return f"{float(value):.1f}"
    if fmt == "2f":
        return f"{float(value):.2f}"
    if fmt == "3f":
        return f"{float(value):.3f}"
    return str(value)


def _format_delta_cell(value: float | None, fmt: str) -> str:
    """Format one cell of a delta column (signed, same precision as the source)."""
    if value is None or pd.isna(value):
        return "-"
    if fmt == "comma":
        return f"{float(value):+,.0f}"
    if fmt == "1f":
        return f"{float(value):+.1f}"
    if fmt == "2f":
        return f"{float(value):+.2f}"
    if fmt == "3f":
        return f"{float(value):+.3f}"
    return str(value)


def _safe_delta(a: float | None, b: float | None) -> float | None:
    """a - b; None if either side is missing."""
    if a is None or b is None or pd.isna(a) or pd.isna(b):
        return None
    return float(a) - float(b)


def _hourly_query_row(
    group: str,
    source: str,
    feature: str,
    region: str,
    hourly: dict[int, float],
    fmt: str,
) -> dict:
    """Build one hourly query-feature row with OnPk/OffPk/Flat summaries."""
    on_vals = [hourly[h] for h in ONPEAK_HOURS if h in hourly and pd.notna(hourly[h])]
    off_vals = [hourly[h] for h in OFFPEAK_HOURS if h in hourly and pd.notna(hourly[h])]
    flat_vals = [hourly[h] for h in configs.HOURS if h in hourly and pd.notna(hourly[h])]
    return {
        "group": group,
        "source": source,
        "feature": feature,
        "region": region,
        "hourly": hourly,
        "on": float(np.mean(on_vals)) if on_vals else None,
        "off": float(np.mean(off_vals)) if off_vals else None,
        "flat": float(np.mean(flat_vals)) if flat_vals else None,
        "fmt": fmt,
    }


def _source_label_for(cache_dir: Path | None, *dataset_keys: str) -> str:
    """Short feed label (pjm / meteologica / ice) of the picked parquet."""
    for key in dataset_keys:
        path = _resolve_source_path(cache_dir, key)
        if path is None:
            continue
        stem = path.stem.lower()
        for prefix, label in (
            ("pjm_", "pjm"),
            ("meteologica_", "meteologica"),
            ("ice_python_", "ice"),
            ("ice_", "ice"),
        ):
            if stem.startswith(prefix):
                return label
        return stem.split("_")[0]
    return "—"


def _collect_query_rows(
    target_date: date,
    cache_dir: Path | None,
    include_gas: bool,
    include_renewables: bool,
    include_net_load: bool,
) -> list[dict]:
    """Collect per-feed × per-region hourly rows for the query table."""
    rows: list[dict] = []

    def _slice(df: pd.DataFrame | None, region: str | None) -> pd.DataFrame | None:
        if df is None or len(df) == 0:
            return None
        out = df.copy()
        out["date"] = pd.to_datetime(out["date"]).dt.date
        out = out[out["date"] == target_date]
        if region is not None and "region" in out.columns:
            out = out[out["region"].astype(str) == region]
        return out if len(out) > 0 else None

    def _add_per_region_rows(
        group: str,
        feature: str,
        value_col: str,
        df_pjm: pd.DataFrame | None,
        df_meteo: pd.DataFrame | None,
        pjm_key: str,
        meteo_key: str,
        fmt: str,
        df_pjm_has_no_region: bool = False,
    ) -> None:
        for region in configs.LOAD_REGIONS:
            if region == "RTO":
                df = _slice(df_pjm, region=None if df_pjm_has_no_region else "RTO")
                source = _source_label_for(cache_dir, pjm_key)
            else:
                df = _slice(df_meteo, region)
                source = _source_label_for(cache_dir, meteo_key)
            if df is None or value_col not in df.columns:
                continue
            hourly = dict(zip(df["hour_ending"].astype(int), df[value_col].astype(float)))
            rows.append(_hourly_query_row(group, source, feature, region, hourly, fmt))

    # Load forecast: PJM for RTO, Meteologica for MIDATL/WEST/SOUTH.
    df_pjm_load = _safe_load_for_print(loader.load_load_forecast, cache_dir)
    df_meteo_load = _safe_load_for_print(loader.load_meteologica_load_forecast, cache_dir)
    _add_per_region_rows(
        "Load", "forecast_load_mw", "forecast_load_mw",
        df_pjm_load, df_meteo_load,
        pjm_key="load_forecast", meteo_key="meteologica_load_forecast",
        fmt="comma",
    )

    # Solar / wind forecasts: PJM (system-wide, no region col) for RTO; Meteologica for others.
    if include_renewables:
        df_pjm_solar = _safe_load_for_print(loader.load_solar_forecast, cache_dir)
        df_meteo_solar = _safe_load_for_print(loader.load_meteologica_solar_forecast, cache_dir)
        _add_per_region_rows(
            "Solar", "solar_forecast_mw", "solar_forecast",
            df_pjm_solar, df_meteo_solar,
            pjm_key="solar_forecast", meteo_key="meteologica_solar_forecast",
            fmt="comma",
            df_pjm_has_no_region=True,
        )

        df_pjm_wind = _safe_load_for_print(loader.load_wind_forecast, cache_dir)
        df_meteo_wind = _safe_load_for_print(loader.load_meteologica_wind_forecast, cache_dir)
        _add_per_region_rows(
            "Wind", "wind_forecast_mw", "wind_forecast",
            df_pjm_wind, df_meteo_wind,
            pjm_key="wind_forecast", meteo_key="meteologica_wind_forecast",
            fmt="comma",
            df_pjm_has_no_region=True,
        )

    # Net-load forecast: PJM for RTO, Meteologica for MIDATL/WEST/SOUTH.
    if include_net_load:
        df_pjm_nl = _safe_load_for_print(loader.load_net_load_forecast, cache_dir)
        df_meteo_nl = _safe_load_for_print(loader.load_meteologica_net_load_forecast, cache_dir)
        _add_per_region_rows(
            "Net Load", "net_load_forecast_mw", "net_load_forecast_mw",
            df_pjm_nl, df_meteo_nl,
            pjm_key="net_load_forecast", meteo_key="meteologica_net_load_forecast",
            fmt="comma",
        )

    # Gas (per hub).
    if include_gas:
        df_gas = _safe_load_for_print(loader.load_gas_prices_hourly, cache_dir)
        if df_gas is not None and len(df_gas) > 0:
            df_gas = df_gas.copy()
            df_gas["date"] = pd.to_datetime(df_gas["date"]).dt.date
            df_gas = df_gas[df_gas["date"] == target_date]
            source = _source_label_for(cache_dir, "gas_prices_hourly")
            for hub_col, hub_label in _GAS_HUB_LABELS.items():
                if hub_col not in df_gas.columns or not df_gas[hub_col].notna().any():
                    continue
                hourly = dict(zip(df_gas["hour_ending"].astype(int), df_gas[hub_col].astype(float)))
                rows.append(_hourly_query_row("Gas", source, hub_col, hub_label, hourly, "2f"))

    return rows


def _collect_analog_hourly_rows(
    analog_date: date,
    cache_dir: Path | None,
    include_gas: bool,
    include_renewables: bool,
    include_net_load: bool,
) -> list[dict]:
    """Collect realized hourly feature rows for a historical analog date.

    Mirrors the structure of _collect_query_rows but pulls REALIZED actuals
    (rt_load_mw, net_load_mw, solar_gen_mw, wind_gen_mw, gas hub prices).
    """
    rows: list[dict] = []

    def _slice(df: pd.DataFrame | None, region: str | None) -> pd.DataFrame | None:
        if df is None or len(df) == 0:
            return None
        out = df.copy()
        out["date"] = pd.to_datetime(out["date"]).dt.date
        out = out[out["date"] == analog_date]
        if region is not None and "region" in out.columns:
            out = out[out["region"].astype(str) == region]
        return out if len(out) > 0 else None

    df_load = _safe_load_for_print(loader.load_load_rt, cache_dir)
    src_load = _source_label_for(cache_dir, "load_rt")
    for region in configs.LOAD_REGIONS:
        sub = _slice(df_load, region)
        if sub is None or "rt_load_mw" not in sub.columns:
            continue
        hourly = dict(zip(sub["hour_ending"].astype(int), sub["rt_load_mw"].astype(float)))
        rows.append(_hourly_query_row("Load", src_load, "rt_load_mw", region, hourly, "comma"))

    df_nl = _safe_load_for_print(loader.load_net_load_actuals, cache_dir)
    src_nl = _source_label_for(cache_dir, "net_load_actual")

    if include_renewables:
        for region in configs.LOAD_REGIONS:
            sub = _slice(df_nl, region)
            if sub is None or "solar_gen_mw" not in sub.columns:
                continue
            vals = pd.to_numeric(sub["solar_gen_mw"], errors="coerce")
            if not vals.notna().any():
                continue
            hourly = dict(zip(sub["hour_ending"].astype(int), vals.astype(float)))
            rows.append(_hourly_query_row("Solar", src_nl, "solar_gen_mw", region, hourly, "comma"))

        for region in configs.LOAD_REGIONS:
            sub = _slice(df_nl, region)
            if sub is None or "wind_gen_mw" not in sub.columns:
                continue
            vals = pd.to_numeric(sub["wind_gen_mw"], errors="coerce")
            if not vals.notna().any():
                continue
            hourly = dict(zip(sub["hour_ending"].astype(int), vals.astype(float)))
            rows.append(_hourly_query_row("Wind", src_nl, "wind_gen_mw", region, hourly, "comma"))

    if include_net_load:
        for region in configs.LOAD_REGIONS:
            sub = _slice(df_nl, region)
            if sub is None or "net_load_mw" not in sub.columns:
                continue
            vals = pd.to_numeric(sub["net_load_mw"], errors="coerce")
            if not vals.notna().any():
                continue
            hourly = dict(zip(sub["hour_ending"].astype(int), vals.astype(float)))
            rows.append(_hourly_query_row("Net Load", src_nl, "net_load_mw", region, hourly, "comma"))

    if include_gas:
        df_gas = _safe_load_for_print(loader.load_gas_prices_hourly, cache_dir)
        src_gas = _source_label_for(cache_dir, "gas_prices_hourly")
        if df_gas is not None and len(df_gas) > 0:
            df_gas = df_gas.copy()
            df_gas["date"] = pd.to_datetime(df_gas["date"]).dt.date
            df_gas = df_gas[df_gas["date"] == analog_date]
            for hub_col, hub_label in _GAS_HUB_LABELS.items():
                if hub_col not in df_gas.columns or not df_gas[hub_col].notna().any():
                    continue
                hourly = dict(zip(df_gas["hour_ending"].astype(int), df_gas[hub_col].astype(float)))
                rows.append(_hourly_query_row("Gas", src_gas, hub_col, hub_label, hourly, "2f"))

    return rows


def _print_analog_hourly_block(
    target_rows: list[dict],
    analog_rows: list[dict],
) -> None:
    """Print one Target/Analog/Delta hourly comparison block.

    Pairs target and analog rows by (group, region). Three rows printed per pair:
    Target (forecast values), Analog (realized values), Delta (target - analog).
    Gas rows pair on the hub label (Region/Hub column).
    """
    analog_idx: dict[tuple[str, str], dict] = {(r["group"], r["region"]): r for r in analog_rows}

    group_w, region_w, series_w, he_w, sum_w = 10, 12, 8, 7, 9
    header = (
        f"{'Group':<{group_w}} {'Region/Hub':<{region_w}} {'Series':<{series_w}}"
    )
    for h in configs.HOURS:
        header += f" {('HE'+str(h)):>{he_w}}"
    header += f" {'OnPk':>{sum_w}} {'OffPk':>{sum_w}} {'Flat':>{sum_w}}"

    print(header)
    print("-" * len(header))

    last_group: str | None = None
    for tr in target_rows:
        if tr["group"] not in ("Load", "Solar", "Wind", "Net Load", "Gas"):
            continue
        ar = analog_idx.get((tr["group"], tr["region"]))
        if ar is None:
            continue
        if last_group is not None and tr["group"] != last_group:
            print("-" * len(header))
        group_cell = tr["group"] if tr["group"] != last_group else ""
        last_group = tr["group"]

        # Target row
        line = f"{group_cell:<{group_w}} {tr['region']:<{region_w}} {'Target':<{series_w}}"
        for h in configs.HOURS:
            line += f" {_format_query_cell(tr['hourly'].get(h), tr['fmt']):>{he_w}}"
        line += f" {_format_query_cell(tr['on'],   tr['fmt']):>{sum_w}}"
        line += f" {_format_query_cell(tr['off'],  tr['fmt']):>{sum_w}}"
        line += f" {_format_query_cell(tr['flat'], tr['fmt']):>{sum_w}}"
        print(line)

        # Analog row
        line = f"{'':<{group_w}} {'':<{region_w}} {'Analog':<{series_w}}"
        for h in configs.HOURS:
            line += f" {_format_query_cell(ar['hourly'].get(h), ar['fmt']):>{he_w}}"
        line += f" {_format_query_cell(ar['on'],   ar['fmt']):>{sum_w}}"
        line += f" {_format_query_cell(ar['off'],  ar['fmt']):>{sum_w}}"
        line += f" {_format_query_cell(ar['flat'], ar['fmt']):>{sum_w}}"
        print(line)

        # Delta row
        line = f"{'':<{group_w}} {'':<{region_w}} {'Delta':<{series_w}}"
        for h in configs.HOURS:
            d = _safe_delta(tr["hourly"].get(h), ar["hourly"].get(h))
            line += f" {_format_delta_cell(d, tr['fmt']):>{he_w}}"
        line += f" {_format_delta_cell(_safe_delta(tr['on'],   ar['on']),   tr['fmt']):>{sum_w}}"
        line += f" {_format_delta_cell(_safe_delta(tr['off'],  ar['off']),  tr['fmt']):>{sum_w}}"
        line += f" {_format_delta_cell(_safe_delta(tr['flat'], ar['flat']), tr['fmt']):>{sum_w}}"
        print(line)


def _region_from_feature_col(col: str) -> str:
    """Extract region tag from a per-region feature column name (e.g. _rto -> RTO)."""
    for region in configs.LOAD_REGIONS:
        if col.endswith(f"_{region.lower()}"):
            return region
    return "-"


def _print_analog_daily_block(
    query: pd.Series,
    pool_row: pd.Series,
    feature_weights: dict[str, float],
) -> None:
    """Print Target/Analog/Delta for the daily features that feed the KNN distance."""
    g_w, f_w, r_w, val_w, pct_w = 16, 32, 8, 13, 9
    header = (
        f"  {'Group':<{g_w}} {'Feature':<{f_w}} {'Region':<{r_w}} "
        f"{'Target':>{val_w}} {'Analog':>{val_w}} {'Delta':>{val_w}} {'%Chg':>{pct_w}}"
    )
    print()
    print(header)
    print("  " + "-" * (len(header) - 2))

    last_group: str | None = None
    for group, cols in configs.FEATURE_GROUPS.items():
        if group == "calendar_dow":
            continue
        if float(feature_weights.get(group, 0.0)) <= 0:
            continue
        if last_group is not None and group != last_group:
            print("  " + "." * (len(header) - 2))

        for col in cols:
            t = query.get(col) if col in query.index else None
            a = pool_row.get(col) if col in pool_row.index else None
            region = _region_from_feature_col(col)

            t_has = t is not None and pd.notna(t)
            a_has = a is not None and pd.notna(a)
            t_str = f"{float(t):,.2f}" if t_has else "-"
            a_str = f"{float(a):,.2f}" if a_has else "-"
            if t_has and a_has:
                delta = float(t) - float(a)
                delta_str = f"{delta:+,.2f}"
                pct_str = f"{(delta / float(a) * 100):+.2f}%" if float(a) != 0 else "-"
            else:
                delta_str = "-"
                pct_str = "-"

            group_cell = group if group != last_group else ""
            last_group = group
            print(
                f"  {group_cell:<{g_w}} {col:<{f_w}} {region:<{r_w}} "
                f"{t_str:>{val_w}} {a_str:>{val_w}} {delta_str:>{val_w}} {pct_str:>{pct_w}}"
            )


def _print_top5_analog_comparison(
    analogs: pd.DataFrame,
    pool: pd.DataFrame,
    query: pd.Series,
    target_date: date,
    cache_dir: Path | None,
    feature_weights: dict[str, float],
    include_gas: bool,
    include_renewables: bool,
    include_net_load: bool,
) -> None:
    """Print Target/Analog/Delta hourly + daily comparison for top-5 analogs."""
    if len(analogs) == 0:
        return

    target_rows = _collect_query_rows(
        target_date, cache_dir, include_gas, include_renewables, include_net_load,
    )

    pool_dates = pd.to_datetime(pool["date"]).dt.date
    pool_by_date = pool.set_index(pool_dates)

    print_header(
        f"TOP-5 ANALOG COMPARISON  |  Target: {_format_target_label(target_date)}",
        length=120,
    )

    top5 = analogs.head(min(5, len(analogs)))
    n_top = len(top5)
    for idx, (_, row) in enumerate(top5.iterrows()):
        rank = int(row.get("rank", 0))
        analog_date = pd.to_datetime(row["date"]).date()
        distance = float(row.get("distance", float("nan")))
        weight = float(row.get("weight", float("nan")))

        accent = _ANALOG_PALETTE[idx % len(_ANALOG_PALETTE)]
        reset = Colors.RESET if accent else ""
        bold = Colors.BOLD if supports_color() else ""

        header_line = (
            f"  --- Analog #{rank}  {_format_target_label(analog_date)}  "
            f"distance={distance:.4f}  weight={weight:.4%} ---"
        )
        print(f"\n{accent}{bold}{header_line}{reset}")
        print(f"{accent}  {'=' * (len(header_line) - 2)}{reset}")

        analog_rows = _collect_analog_hourly_rows(
            analog_date, cache_dir, include_gas, include_renewables, include_net_load,
        )
        _print_analog_hourly_block(target_rows, analog_rows)

        if analog_date in pool_by_date.index:
            pool_row = pool_by_date.loc[analog_date]
            if isinstance(pool_row, pd.DataFrame):
                pool_row = pool_row.iloc[0]
            _print_analog_daily_block(query, pool_row, feature_weights)
        else:
            logger.warning(
                "Analog %s not found in pool -- skipping daily comparison", analog_date,
            )

        # Per-analog divider (blank line + colored separator) between blocks.
        if idx < n_top - 1:
            print(f"\n{accent}  {'-' * 116}{reset}")

    print_divider("=", 120, dim=False)


def _print_query_features(
    query: pd.Series,
    target_date: date,
    cache_dir: Path | None,
    include_gas: bool,
    include_renewables: bool,
    include_net_load: bool,
) -> None:
    """Print hourly query feature values per feed × region/hub for target date."""
    rows = _collect_query_rows(
        target_date, cache_dir, include_gas, include_renewables, include_net_load,
    )

    group_w, src_w, feat_w, region_w, he_w, sum_w = 10, 12, 22, 12, 7, 9
    header = (
        f"{'Group':<{group_w}} {'Source':<{src_w}} "
        f"{'Feature':<{feat_w}} {'Region/Hub':<{region_w}}"
    )
    for h in configs.HOURS:
        header += f" {('HE'+str(h)):>{he_w}}"
    header += f" {'OnPk':>{sum_w}} {'OffPk':>{sum_w}} {'Flat':>{sum_w}}"

    print_header(
        f"QUERY FEATURE VALUES  |  Target: {_format_target_label(target_date)}",
        length=len(header),
    )
    print(header)
    print("-" * len(header))

    last_group: str | None = None
    last_source: str | None = None
    for r in rows:
        if last_group is not None and r["group"] != last_group:
            print("-" * len(header))
            last_source = None  # always re-print Source on first row of a new group
        group_cell = r["group"] if r["group"] != last_group else ""
        source_cell = r["source"] if r["source"] != last_source else ""
        last_group = r["group"]
        last_source = r["source"]
        line = (
            f"{group_cell:<{group_w}} {source_cell:<{src_w}} "
            f"{r['feature']:<{feat_w}} {r['region']:<{region_w}}"
        )
        for h in configs.HOURS:
            line += f" {_format_query_cell(r['hourly'].get(h), r['fmt']):>{he_w}}"
        line += f" {_format_query_cell(r['on'],   r['fmt']):>{sum_w}}"
        line += f" {_format_query_cell(r['off'],  r['fmt']):>{sum_w}}"
        line += f" {_format_query_cell(r['flat'], r['fmt']):>{sum_w}}"
        print(line)

    # Daily-only features (no hourly grain) — small footer.
    daily_only: list[tuple[str, str, float | None, str]] = [
        ("outage_total_mw",     configs.LOAD_REGION,  query.get("outage_total_mw"),     "comma"),
        ("outage_forced_mw",    configs.LOAD_REGION,  query.get("outage_forced_mw"),    "comma"),
        ("outage_forced_share", configs.LOAD_REGION,  query.get("outage_forced_share"), "3f"),
    ]
    daily_present = [(label, region, val, fmt) for label, region, val, fmt in daily_only
                     if val is not None and not pd.isna(val)]
    if daily_present:
        print_section("Daily-only features (no hourly grain)")
        for label, region, val, fmt in daily_present:
            print(f"    {label:<{feat_w}} {region:<{region_w}} {_format_query_cell(val, fmt):>{sum_w}}")

    print_divider("=", len(header), dim=False)


def _format_target_label(target_date: date) -> str:
    dow = DAY_ABBR[target_date.weekday()]
    return f"{target_date} ({dow})"


def _print_feature_vector(
    query: pd.Series,
    feature_weights: dict[str, float],
    target_date: date,
) -> None:
    """Print the daily-aggregated feature vector that actually feeds the KNN distance.

    Bridges QUERY FEATURE VALUES (raw hourly per region/hub) and FORECAST
    CONFIGURATION (weight bars) by listing every (group, feature) the model
    consumes, its scalar value, the group weight, and a coverage flag.
    """
    print_header(
        f"FEATURE VECTOR (KNN INPUT)  |  Target: {_format_target_label(target_date)}",
        length=90,
    )

    g_w, f_w, v_w, wt_w, st_w = 18, 26, 14, 8, 12
    header = (
        f"  {'Group':<{g_w}} {'Feature':<{f_w}} "
        f"{'Value':>{v_w}} {'Weight':>{wt_w}}  {'Status':<{st_w}}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))

    n_groups_active = 0
    n_features_total = 0
    n_features_filled = 0
    last_group: str | None = None

    for group, cols in configs.FEATURE_GROUPS.items():
        weight = float(feature_weights.get(group, 0.0))
        group_active = weight > 0
        if group_active:
            n_groups_active += 1

        if last_group is not None and group != last_group:
            print("  " + "." * (len(header) - 2))
        last_group = group

        for col in cols:
            n_features_total += 1
            val = query.get(col) if col in query.index else None
            has_value = val is not None and not pd.isna(val)

            if has_value:
                n_features_filled += 1
                value_str = f"{float(val):,.2f}"
                status = "ok"
                color = ""
            else:
                value_str = "-"
                if group_active:
                    status = "MISSING"
                    color = Colors.BRIGHT_YELLOW if supports_color() else ""
                else:
                    status = "off (w=0)"
                    color = Colors.DIM if supports_color() else ""

            weight_str = f"{weight:.2f}" if group_active else "—"
            line = (
                f"  {group:<{g_w}} {col:<{f_w}} "
                f"{value_str:>{v_w}} {weight_str:>{wt_w}}  {status:<{st_w}}"
            )
            if color:
                line = f"{color}{line}{Colors.RESET}"
            print(line)

    n_groups_total = len(configs.FEATURE_GROUPS)
    print("  " + "-" * (len(header) - 2))
    print(
        f"  Active groups: {n_groups_active}/{n_groups_total}  |  "
        f"Filled features: {n_features_filled}/{n_features_total}"
    )
    print_divider("=", 90, dim=False)


def _print_config(
    target_date: date,
    config: configs.ForwardOnlyKNNConfig,
    base_weights: dict[str, float],
    effective_weights: dict[str, float],
    disabled_groups: list[str],
) -> None:
    """Print the resolved forecast configuration."""
    target_label = _format_target_label(target_date)
    window = config.season_window_days
    win_start = target_date - timedelta(days=window)
    win_end = target_date + timedelta(days=window)

    print_header("FORWARD-ONLY KNN — FORECAST CONFIGURATION", length=90)
    print(f"\n  Target            {target_label}")
    print(f"  Hub               {config.hub}")
    print(f"  Schema            {config.schema}")
    print(f"  N analogs         {config.n_analogs}")
    print(f"  Weight method     {config.weight_method}")
    print(f"  Season window     +/-{window}d  ({win_start.strftime('%b %d')} - {win_end.strftime('%b %d')})")
    print(f"  Same-DOW group    {config.same_dow_group}")
    print(f"  Exclude holidays  {config.exclude_holidays}")
    print(f"  Min pool size     {config.min_pool_size}")
    day_type = configs._dow_key_for(target_date)
    profiles = config.resolved_day_type_profiles() if config.use_day_type_profiles else {}
    profile_keys = sorted((profiles.get(day_type) or {}).keys())
    profile_label = ",".join(profile_keys) if profile_keys else "-"
    print(f"  Day-type          {day_type}  (override: {profile_label})")
    if config.apply_outage_regime_filter:
        print(
            f"  Outage filter     z-score within +/-{config.outage_tolerance_std:.1f} std "
            f"on {config.outage_filter_col}"
        )
    else:
        print("  Outage filter     disabled")

    active = {k: v for k, v in sorted(effective_weights.items()) if v > 0}
    print_section("Feature Weights (active)")
    for name, w in sorted(active.items(), key=lambda x: -x[1]):
        bar = "#" * int(round(w * 4))
        print(f"  {name:<32} {w:>5.2f}  {bar}")

    if disabled_groups:
        print_section("Auto-disabled Groups")
        for g in disabled_groups:
            base = float(base_weights.get(g, 0.0))
            print(f"  {g:<32} (was {base:>4.2f})")
    print_divider("=", 90, dim=False)


def _print_analogs(analogs: pd.DataFrame, target_date: date) -> None:
    """Print the top analog days table."""
    if len(analogs) == 0:
        return

    target_label = _format_target_label(target_date)
    print_header(f"ANALOG DAYS  |  Target: {target_label}", length=90)

    display = analogs.head(min(10, len(analogs))).copy()
    display["date"] = pd.to_datetime(display["date"]).dt.strftime("%a %b-%d %Y")
    if "distance" in display.columns:
        display["distance"] = display["distance"].map("{:.4f}".format)
    if "weight" in display.columns:
        display["weight"] = display["weight"].map("{:.4f}".format)

    cols = [c for c in ["rank", "date", "distance", "weight"] if c in display.columns]
    header = "  " + "  ".join(f"{c:<14}" for c in cols)
    print(header)
    print("  " + "-" * (len(header) - 2))
    for _, row in display.iterrows():
        print("  " + "  ".join(f"{str(row[c]):<14}" for c in cols))

    top5_weight = float(analogs.head(5)["weight"].sum()) if "weight" in analogs.columns else 0.0
    if "distance" in analogs.columns:
        d_min = float(pd.to_numeric(analogs["distance"], errors="coerce").min())
        d_max = float(pd.to_numeric(analogs["distance"], errors="coerce").max())
        print(
            f"\n  Total analogs: {len(analogs)} | "
            f"Top-5 weight sum: {top5_weight:.2%} | "
            f"Distance range: {d_min:.4f} — {d_max:.4f}"
        )


def _print_table(table: pd.DataFrame, title: str) -> None:
    """Print the Actual/Forecast/Error table with row coloring."""
    print_header(title, length=120)

    header = f"{'Date':<12} {'Type':<10}"
    for h in configs.HOURS:
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    for _, row in table.iterrows():
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in configs.HOURS:
            val = row[f"HE{h}"]
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        line += f" {row['OnPeak']:>7.2f}" if pd.notna(row["OnPeak"]) else f" {'':>7}"
        line += f" {row['OffPeak']:>7.2f}" if pd.notna(row["OffPeak"]) else f" {'':>7}"
        line += f" {row['Flat']:>7.2f}" if pd.notna(row["Flat"]) else f" {'':>7}"
        style = _ROW_STYLES.get(row["Type"], "")
        if style and supports_color():
            line = f"{style}{line}{_RS}"
        print(line)

    print_divider("=", 120, dim=False)


def run_forecast(
    target_date: date | None = None,
    n_analogs: int = configs.DEFAULT_N_ANALOGS,
    config: configs.ForwardOnlyKNNConfig | None = None,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
    print_analog_comparison: bool = True,
) -> dict:
    """Run D+1 forward-only KNN forecast."""
    if config is None:
        config = configs.ForwardOnlyKNNConfig(n_analogs=n_analogs)

    if target_date is None:
        target_date = config.resolved_target_date()
    else:
        target_date = pd.to_datetime(target_date).date()

    # Apply weekend/weekday profile (no-op for weekday or when disabled).
    config, day_type = config.with_day_type_overrides(target_date)

    horizon_offset = max((target_date - date.today()).days, 1)
    include_gas = horizon_offset <= config.gas_feature_max_horizon_days
    include_outages = horizon_offset <= config.outage_feature_max_horizon_days
    include_renewables = horizon_offset <= config.renewable_feature_max_horizon_days
    include_net_load = horizon_offset <= config.net_load_feature_max_horizon_days
    weights = config.resolved_feature_weights(
        include_gas=include_gas,
        include_outages=include_outages,
        include_renewables=include_renewables,
        include_net_load=include_net_load,
    )

    logger.info("=" * 60)
    logger.info(
        "Forward-only KNN: target=%s hub=%s n_analogs=%d horizon=D+%d day_type=%s",
        target_date, config.hub, config.n_analogs, horizon_offset, day_type,
    )
    logger.info(
        "Horizon-conditional features: gas=%s outages=%s renewables=%s net_load=%s",
        include_gas, include_outages, include_renewables, include_net_load,
    )
    logger.info("=" * 60)

    logger.info("Building historical pool...")
    pool = build_pool(
        schema=config.schema,
        hub=config.hub,
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )
    _validate_pool(pool, target_date)

    logger.info("Building query feature row for %s...", target_date)
    query = build_query_row(
        target_date=target_date,
        schema=config.schema,
        include_gas=include_gas,
        include_outages=include_outages,
        include_renewables=include_renewables,
        include_net_load=include_net_load,
        cache_dir=cache_dir,
        cache_enabled=cache_enabled,
        cache_ttl_hours=cache_ttl_hours,
        force_refresh=force_refresh,
    )

    _print_forecast_sources(
        cache_dir,
        target_date,
        flags={
            "include_gas": include_gas,
            "include_outages": include_outages,
            "include_renewables": include_renewables,
            "include_net_load": include_net_load,
        },
    )
    _print_query_features(
        query,
        target_date,
        cache_dir=cache_dir,
        include_gas=include_gas,
        include_renewables=include_renewables,
        include_net_load=include_net_load,
    )
    _print_feature_vector(query, weights, target_date)

    query_validation = _validate_query_features(query, weights)
    n_active = sum(1 for v in weights.values() if v > 0)
    n_clean = sum(1 for g, r in query_validation.items() if weights.get(g, 0) > 0 and not r["missing"])
    logger.info("Query feature coverage: %d/%d active groups have full coverage", n_clean, n_active)

    preflight = run_preflight(
        query=query,
        pool=_season_window_filter_for_preflight(pool, target_date, config.season_window_days),
        target_date=target_date,
        feature_weights=weights,
        min_pool_size=config.min_pool_size,
    )
    effective_weights, disabled_groups = _derive_effective_weights(
        base_weights=weights,
        missing_query_groups=preflight.missing_query_groups,
        low_pool_groups=preflight.low_pool_groups,
    )
    if disabled_groups:
        logger.warning(
            "Auto-disabling %d feature group(s) due to coverage gaps: %s",
            len(disabled_groups), disabled_groups,
        )

    _print_config(target_date, config, weights, effective_weights, disabled_groups)

    logger.info("Finding %d analogs...", config.n_analogs)
    analogs = find_twins(
        query=query,
        pool=pool,
        target_date=target_date,
        n_analogs=config.n_analogs,
        feature_weights=effective_weights,
        min_pool_size=config.min_pool_size,
        same_dow_group=config.same_dow_group,
        exclude_holidays=config.exclude_holidays,
        season_window_days=config.season_window_days,
        recency_half_life_days=config.recency_half_life_days,
        weight_method=config.weight_method,
        apply_outage_regime_filter=config.apply_outage_regime_filter,
        outage_tolerance_std=config.outage_tolerance_std,
        outage_filter_col=config.outage_filter_col,
    )

    if len(analogs) == 0:
        logger.error("No analogs returned — aborting forecast")
        return {"error": "No analogs found", "forecast_date": str(target_date)}

    if "weight" in analogs.columns:
        top5 = float(analogs.head(5)["weight"].sum())
        logger.info("Found %d analogs (top-5 weight sum: %.2f%%)", len(analogs), top5 * 100)
    _print_analogs(analogs, target_date)
    if print_analog_comparison:
        _print_top5_analog_comparison(
            analogs=analogs,
            pool=pool,
            query=query,
            target_date=target_date,
            cache_dir=cache_dir,
            feature_weights=effective_weights,
            include_gas=include_gas,
            include_renewables=include_renewables,
            include_net_load=include_net_load,
        )

    quantiles = config.resolved_quantiles()
    df_forecast = _hourly_forecast_from_analogs(analogs, quantiles)
    _validate_hourly_forecast(df_forecast)
    forecast_hourly = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast["point_forecast"]))

    actual_hourly = _actuals_from_pool(pool, target_date)
    has_actuals = actual_hourly is not None
    if has_actuals:
        logger.info("Actuals available for %s — table will include Error row", target_date)
    else:
        logger.info("No actuals for %s — forecast-only output", target_date)

    output_table = _build_output_table(target_date, forecast_hourly, actual_hourly)

    q_rows: list[dict] = []
    for q in quantiles:
        q_col = f"q_{q:.2f}"
        if q_col not in df_forecast.columns:
            continue
        row = {"Date": target_date, "Type": _quantile_label(q)}
        hourly = dict(zip(df_forecast["hour_ending"].astype(int), df_forecast[q_col]))
        for h in configs.HOURS:
            row[f"HE{h}"] = hourly.get(h)
        q_rows.append(_add_summary_cols(row))

    q_cols = ["Date", "Type"] + [f"HE{h}" for h in configs.HOURS] + ["OnPeak", "OffPeak", "Flat"]
    quantiles_table = pd.DataFrame(q_rows, columns=q_cols)

    forecast_rows = output_table[output_table["Type"] == "Forecast"].iloc[0:1].copy()
    p50_idx = quantiles_table[quantiles_table["Type"] == "P50"].index
    if len(forecast_rows) > 0 and len(p50_idx) > 0:
        pos = int(p50_idx[0]) + 1
        quantiles_table = pd.concat(
            [quantiles_table.iloc[:pos], forecast_rows, quantiles_table.iloc[pos:]],
        ).reset_index(drop=True)

    _print_table(output_table, f"DA LMP FORECAST — {config.hub} ($/MWh)")
    _print_table(quantiles_table, "QUANTILE BANDS ($/MWh)")

    preflight_dict = preflight.as_dict()
    preflight_dict["effective_feature_weights"] = effective_weights
    preflight_dict["disabled_feature_groups"] = disabled_groups
    preflight_dict["query_feature_validation"] = {
        g: {"coverage": r["coverage"], "missing": r["missing"]}
        for g, r in query_validation.items()
        if weights.get(g, 0) > 0
    }

    return {
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "analogs": analogs,
        "metrics": None,
        "forecast_date": str(target_date),
        "reference_date": str(target_date - timedelta(days=1)),
        "has_actuals": has_actuals,
        "n_analogs_used": len(analogs),
        "df_forecast": df_forecast,
        "scenario": "forward_only_knn",
        "preflight": preflight_dict,
    }


def run(*args, **kwargs) -> dict:
    """Backward-compatible alias."""
    return run_forecast(*args, **kwargs)


if __name__ == "__main__":
    from utils.logging_utils import init_logging

    _MODELLING_ROOT = Path(__file__).resolve().parents[3]
    init_logging(name="forward_only_knn_forecast", log_dir=_MODELLING_ROOT / "logs")

    # target_date = datetime.now().date()
    # result = run_forecast(target_date=target_date, print_analog_comparison=False)
    
    result = run_forecast(print_analog_comparison=False)
    if "error" in result:
        logger.error(result["error"])
