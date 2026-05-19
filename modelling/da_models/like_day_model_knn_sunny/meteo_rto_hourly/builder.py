"""Pool and query builders for the ``meteo_rto_hourly`` Sunny variant.

The pool is the same historical long-format frame the sibling
``pjm_rto_hourly`` builds (delegates to ``_shared.build_pool_from_spec``).
What changes is the *query* — the 24-row-per-target-date feature frame
that gets matched against pool rows:

  - Single-day query (``build_query_row``) sources load / solar / wind /
    net_load from the *latest published* Meteologica regional vintage
    (RTO), so any delivery date the vintage covers (typically out to
    D+10..D+14) is usable, not just D+1.
  - Multi-day query (``build_horizon_query_rows``) returns one query
    frame per target date in ``target_dates``. The Meteologica vintage
    is fetched once, then sliced per date. Load ramps are computed
    across the full chain (yesterday RT → D+1 → D+2 → ...) so HE1's 1h
    ramp on each target date references the prior day's HE24. Outages
    and gas — daily feeds that don't reach the horizon — are
    forward-filled from the last known value.

The pool uses the *same* domains as the spec (so the column set the
engine z-scores against is consistent), but its source priority for
historical rows is unchanged from ``pjm_rto_hourly`` (RT actuals first,
lead-1 forecasts as fallback). Only the target query differs.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.common.data import loader
from da_models.like_day_model_knn_sunny import _shared, calendar as _calendar, configs
from da_models.like_day_model_knn_sunny.domains import all_feature_cols

logger = logging.getLogger(__name__)

RTO = "RTO"
HOURS: list[int] = list(range(1, 25))


def build_pool(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    label_source: str = configs.LABEL_SOURCE,
    cache_dir: Path | None = configs.CACHE_DIR,
    spec: configs.ModelSpec = configs.METEO_RTO_HOURLY_SUNNY_SPEC,
) -> pd.DataFrame:
    """Same historical pool as the sibling variant, parameterized to the meteo spec."""
    _ = schema
    return _shared.build_pool_from_spec(
        spec=spec,
        hub=hub,
        label_source=label_source,
        cache_dir=cache_dir,
    )


def filter_pool_by_year_months(
    pool: pd.DataFrame,
    year_months: dict[int, list[int]] | None,
) -> pd.DataFrame:
    """Restrict the analog pool to (year, month) buckets in ``year_months``.

    ``year_months`` shape: ``{year: [month, ...]}``. Example
    ``{2026: [4], 2025: [5, 6], 2024: [5, 6]}`` keeps April 2026 plus
    May / June from 2024 and 2025. ``None`` returns the pool unchanged.

    Empty result raises -- the engine cannot match against zero rows and
    a silent empty pool would produce nonsense.
    """
    if not year_months:
        return pool
    if len(pool) == 0:
        return pool
    dt = pd.to_datetime(pool["date"])
    years = dt.dt.year.to_numpy()
    months = dt.dt.month.to_numpy()
    keep = np.zeros(len(pool), dtype=bool)
    for y, ms in year_months.items():
        for m in ms:
            keep |= (years == int(y)) & (months == int(m))
    out = pool.loc[keep].reset_index(drop=True)
    if len(out) == 0:
        raise ValueError(
            f"filter_pool_by_year_months: pool is empty after applying "
            f"{year_months!r}. Pool date range was "
            f"{pool['date'].min()} .. {pool['date'].max()}."
        )
    return out


# ── Meteologica latest-vintage pivot ───────────────────────────────────


def _load_meteo_rto_latest(cache_dir: Path | None) -> pd.DataFrame:
    """Latest Meteologica supply-demand for RTO, columns standardized to
    the Sunny scalar names (``load_mw_at_hour``, ``solar_at_hour``,
    ``wind_at_hour``, ``net_load_at_hour``).

    Returns one row per ``(date, hour_ending)`` over every delivery date
    the latest vintage covers (typically ~D-0..D+14, full-24-HE-coverage
    gate already applied in the loader).
    """
    df = loader.load_meteologica_supply_demand_coalesced(
        cache_dir=cache_dir, latest_only=True
    )
    if df is None or len(df) == 0:
        return pd.DataFrame(
            columns=[
                "date",
                "hour_ending",
                "load_mw_at_hour",
                "solar_at_hour",
                "wind_at_hour",
                "net_load_at_hour",
            ]
        )
    sub = df[df["region"].astype(str) == RTO].copy()
    sub["date"] = pd.to_datetime(sub["date"]).dt.date
    sub["hour_ending"] = pd.to_numeric(sub["hour_ending"], errors="coerce").astype(
        "Int64"
    )
    sub = sub.dropna(subset=["date", "hour_ending"])
    sub["hour_ending"] = sub["hour_ending"].astype(int)
    sub = sub.rename(
        columns={
            "load_mw": "load_mw_at_hour",
            "solar_mw": "solar_at_hour",
            "wind_mw": "wind_at_hour",
            "net_load_mw": "net_load_at_hour",
        }
    )
    keep = [
        "date",
        "hour_ending",
        "load_mw_at_hour",
        "solar_at_hour",
        "wind_at_hour",
        "net_load_at_hour",
    ]
    return (
        sub[keep]
        .drop_duplicates(subset=["date", "hour_ending"], keep="first")
        .sort_values(["date", "hour_ending"])
        .reset_index(drop=True)
    )


# ── WSI temperature forecast (per-date slice from a single load) ───────


def _load_wsi_temp_horizon(cache_dir: Path | None) -> pd.DataFrame:
    """Forecast-first WSI hourly temperature, sliced to ``(date,
    hour_ending, temp_at_hour)``. One read; the caller slices per
    target date. Returns an empty frame when the WSI forecast mart is
    missing or unreadable so the rest of the query path still works
    (the day's ``temp_at_hour`` simply stays NaN and the strip's
    feat_ok flag fires).
    """
    df = loader.load_weather_forecast_hourly(cache_dir=cache_dir)
    if df is None or len(df) == 0 or "temp" not in df.columns:
        return pd.DataFrame(columns=["date", "hour_ending", "temp_at_hour"])
    out = df[["date", "hour_ending", "temp"]].rename(columns={"temp": "temp_at_hour"})
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out["hour_ending"] = pd.to_numeric(out["hour_ending"], errors="coerce").astype(
        "Int64"
    )
    out = out.dropna(subset=["date", "hour_ending"]).copy()
    out["hour_ending"] = out["hour_ending"].astype(int)
    return (
        out.drop_duplicates(subset=["date", "hour_ending"], keep="first")
        .sort_values(["date", "hour_ending"])
        .reset_index(drop=True)
    )


# ── Load ramps over the chain (yesterday RT + meteo D+1..D+N) ──────────


def _prev_day_rt_load(prev_date: date, cache_dir: Path | None) -> pd.DataFrame:
    """RT load for the day immediately before the first target — used as
    HE1's 1h-ramp predecessor for D+1. Empty frame when RT isn't in
    cache for that date."""
    df = loader.load_load_rt(cache_dir=cache_dir)
    if df is None or len(df) == 0 or "rt_load_mw" not in df.columns:
        return pd.DataFrame(columns=["date", "hour_ending", "load_mw_at_hour"])
    rt = df.copy()
    if "region" in rt.columns:
        rt = rt[rt["region"].astype(str) == RTO]
    rt["date"] = pd.to_datetime(rt["date"]).dt.date
    rt = rt[rt["date"] == prev_date]
    if len(rt) == 0:
        return pd.DataFrame(columns=["date", "hour_ending", "load_mw_at_hour"])
    out = rt[["date", "hour_ending", "rt_load_mw"]].rename(
        columns={"rt_load_mw": "load_mw_at_hour"}
    )
    out["hour_ending"] = pd.to_numeric(out["hour_ending"], errors="coerce").astype(int)
    return out.sort_values(["date", "hour_ending"]).reset_index(drop=True)


def _add_ramps_along_chain(load_chain: pd.DataFrame) -> pd.DataFrame:
    """Compute 1h / 3h load deltas along a sorted ``(date, hour_ending)``
    timeline. Mirrors ``domains._add_load_ramps`` but operates on the
    chain frame so HE1's 1h ramp on each date pulls from the prior day's
    HE24 by construction."""
    chain = load_chain.sort_values(["date", "hour_ending"]).reset_index(drop=True)
    src = chain["load_mw_at_hour"].astype(float).to_numpy()
    shift1 = np.concatenate(([np.nan], src[:-1]))
    shift3 = np.concatenate(([np.nan, np.nan, np.nan], src[:-3]))
    chain["load_ramp_1h_at_hour"] = src - shift1
    chain["load_ramp_3h_at_hour"] = src - shift3
    return chain


# ── Daily-broadcast feeds with forward-fill ────────────────────────────


def _outages_for_dates(
    target_dates: list[date], cache_dir: Path | None
) -> dict[date, float]:
    """Lead-1 outage forecast value per target_date; values past the
    forecast horizon are forward-filled from the last known total."""
    df = loader.load_outages_forecast_history(cache_dir=cache_dir, lead_days=1)
    if df is None or len(df) == 0:
        return dict.fromkeys(target_dates, float("nan"))
    of = df.copy()
    if "region" in of.columns:
        of = of[of["region"].astype(str) == RTO]
    date_col = "forecast_date" if "forecast_date" in of.columns else "date"
    of["date"] = pd.to_datetime(of[date_col]).dt.date
    of = (
        of[["date", "total_outages_mw"]]
        .drop_duplicates(subset=["date"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )
    if len(of) == 0:
        return dict.fromkeys(target_dates, float("nan"))
    by_date = dict(zip(of["date"], pd.to_numeric(of["total_outages_mw"], errors="coerce")))
    last_known: float = float("nan")
    # Walk forward; carry the last available forecast value to fill horizon gaps.
    # ``of`` already covers everything before the targets, so seed from its tail.
    last_known = float(of["total_outages_mw"].iloc[-1])
    out: dict[date, float] = {}
    for d in target_dates:
        v = by_date.get(d)
        if v is not None and pd.notna(v):
            last_known = float(v)
        out[d] = last_known
    return out


def _gas_for_dates(
    target_dates: list[date], cache_dir: Path | None
) -> dict[date, float]:
    """Daily mean gas (across available hubs), forward-filled from the
    last known historical date out to every requested target date."""
    df = loader.load_gas_prices_hourly(cache_dir=cache_dir)
    if df is None or len(df) == 0 or "gas_m3" not in df.columns:
        return dict.fromkeys(target_dates, float("nan"))
    work = df[["date", "gas_m3"]].copy()
    work["date"] = pd.to_datetime(work["date"]).dt.date
    work["gas_m3"] = pd.to_numeric(work["gas_m3"], errors="coerce")
    work = work.dropna(subset=["date", "gas_m3"])
    daily = (
        work.groupby("date", as_index=False)
        .agg(gas_m3_daily_avg=("gas_m3", "mean"))
        .sort_values("date")
        .reset_index(drop=True)
    )
    if len(daily) == 0:
        return dict.fromkeys(target_dates, float("nan"))
    by_date = dict(zip(daily["date"], daily["gas_m3_daily_avg"]))
    last_known = float(daily["gas_m3_daily_avg"].iloc[-1])
    out: dict[date, float] = {}
    for d in target_dates:
        v = by_date.get(d)
        if v is not None and pd.notna(v):
            last_known = float(v)
        out[d] = last_known
    return out


# ── Calendar per date ─────────────────────────────────────────────────


def _calendar_by_date(
    target_dates: list[date], cache_dir: Path | None
) -> dict[date, dict[str, float]]:
    dates_meta = _calendar.load_pjm_dates_daily(cache_dir=cache_dir)
    holidays: dict[date, int] = {}
    if (
        dates_meta is not None
        and len(dates_meta) > 0
        and "is_nerc_holiday" in dates_meta.columns
    ):
        meta = dates_meta[["date", "is_nerc_holiday"]].copy()
        meta["date"] = pd.to_datetime(meta["date"]).dt.date
        holidays = dict(
            zip(meta["date"], meta["is_nerc_holiday"].fillna(0).astype(int).tolist())
        )
    return {
        d: _calendar.compute_sunny_calendar_row(
            d, is_nerc_holiday=bool(holidays.get(d, 0))
        )
        for d in target_dates
    }


# ── Public query builders ──────────────────────────────────────────────


def _empty_24(target_date: date, feature_cols: list[str]) -> pd.DataFrame:
    base = pd.DataFrame({"date": [target_date] * 24, "hour_ending": HOURS})
    for c in feature_cols:
        base[c] = np.nan
    return base


def build_horizon_query_rows(
    target_dates: list[date] | tuple[date, ...],
    cache_dir: Path | None = configs.CACHE_DIR,
    spec: configs.ModelSpec = configs.METEO_RTO_HOURLY_SUNNY_SPEC,
) -> dict[date, pd.DataFrame]:
    """Return ``{target_date: 24-row query frame}`` for every requested date.

    Each frame has the spec's feature columns plus the always-attached
    ``CALENDAR_COLS``. Coverage caveats:

      - Load / solar / wind / net_load come from a single Meteologica
        ``latest_only`` fetch; dates outside the vintage's horizon get
        NaN feature values and the engine will fall back to whatever
        analogs survive z-scoring with partial features.
      - Temperature comes from a single WSI hourly forecast load
        (``load_weather_forecast_hourly``); dates past the WSI horizon
        leave ``temp_at_hour`` NaN. The temperature feature group is
        only included when the spec lists ``temperature_scalar``.
      - Outages and gas are forward-filled from the last known daily
        value (the lead-1 outage forecast typically covers D+1 only;
        gas is settled day-of), so every target date gets a non-NaN
        scalar for those daily-broadcast features.
      - Load ramps are computed over the chain ``[D, D+1, ..., D+N]``
        with yesterday-RT prepended, so HE1's 1h ramp on every target
        date references the prior day's HE24 by construction.
    """
    if not target_dates:
        return {}
    target_dates = sorted(set(target_dates))
    feature_cols = all_feature_cols(spec.domains)

    meteo = _load_meteo_rto_latest(cache_dir)
    wsi_temp = _load_wsi_temp_horizon(cache_dir)
    prev_date = target_dates[0] - timedelta(days=1)
    prev_rt = _prev_day_rt_load(prev_date, cache_dir)

    # Assemble the load chain for ramp computation: yesterday RT first,
    # then every target date's Meteologica load that we have rows for.
    meteo_load = meteo[meteo["date"].isin(target_dates)][
        ["date", "hour_ending", "load_mw_at_hour"]
    ].copy()
    chain_parts: list[pd.DataFrame] = []
    if len(prev_rt) > 0:
        chain_parts.append(prev_rt.assign(__keep=False))
    chain_parts.append(meteo_load.assign(__keep=True))
    chain = pd.concat(chain_parts, ignore_index=True)
    chain = _add_ramps_along_chain(chain)
    ramps = chain.loc[
        chain["__keep"],
        ["date", "hour_ending", "load_ramp_1h_at_hour", "load_ramp_3h_at_hour"],
    ].reset_index(drop=True)

    outages_by_date = _outages_for_dates(list(target_dates), cache_dir)
    gas_by_date = _gas_for_dates(list(target_dates), cache_dir)
    calendar_by_date = _calendar_by_date(list(target_dates), cache_dir)

    extra_cal = [c for c in _shared.CALENDAR_COLS if c not in feature_cols]
    keep_cols = ["date", "hour_ending"] + feature_cols + extra_cal

    out: dict[date, pd.DataFrame] = {}
    for d in target_dates:
        base = _empty_24(d, feature_cols)
        m = meteo[meteo["date"] == d]
        if len(m) > 0:
            base = base.drop(
                columns=[
                    c
                    for c in (
                        "load_mw_at_hour",
                        "solar_at_hour",
                        "wind_at_hour",
                        "net_load_at_hour",
                    )
                    if c in base.columns
                ]
            ).merge(
                m[
                    [
                        "date",
                        "hour_ending",
                        "load_mw_at_hour",
                        "solar_at_hour",
                        "wind_at_hour",
                        "net_load_at_hour",
                    ]
                ],
                on=["date", "hour_ending"],
                how="left",
            )

        if "temp_at_hour" in feature_cols and len(wsi_temp) > 0:
            t = wsi_temp[wsi_temp["date"] == d]
            if len(t) > 0:
                base = base.drop(
                    columns=[c for c in ("temp_at_hour",) if c in base.columns]
                ).merge(
                    t[["date", "hour_ending", "temp_at_hour"]],
                    on=["date", "hour_ending"],
                    how="left",
                )

        r = ramps[ramps["date"] == d]
        if len(r) > 0:
            base = base.drop(
                columns=[
                    c
                    for c in ("load_ramp_1h_at_hour", "load_ramp_3h_at_hour")
                    if c in base.columns
                ]
            ).merge(
                r[
                    [
                        "date",
                        "hour_ending",
                        "load_ramp_1h_at_hour",
                        "load_ramp_3h_at_hour",
                    ]
                ],
                on=["date", "hour_ending"],
                how="left",
            )

        if "outage_total_mw" in feature_cols:
            base["outage_total_mw"] = outages_by_date.get(d, np.nan)
        if "gas_m3_daily_avg" in feature_cols:
            base["gas_m3_daily_avg"] = gas_by_date.get(d, np.nan)

        for k, v in calendar_by_date.get(d, {}).items():
            base[k] = v

        for c in keep_cols:
            if c not in base.columns:
                base[c] = np.nan
        base = base.loc[:, ~base.columns.duplicated()]
        out[d] = (
            base[keep_cols].sort_values("hour_ending").reset_index(drop=True)
        )

    return out


def build_query_row(
    target_date: date,
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    spec: configs.ModelSpec = configs.METEO_RTO_HOURLY_SUNNY_SPEC,
) -> pd.DataFrame:
    """Single-day query for the meteo variant — thin wrapper over
    ``build_horizon_query_rows`` with one target date."""
    _ = schema
    frames = build_horizon_query_rows([target_date], cache_dir=cache_dir, spec=spec)
    return frames.get(
        target_date,
        _empty_24(target_date, all_feature_cols(spec.domains)),
    )
