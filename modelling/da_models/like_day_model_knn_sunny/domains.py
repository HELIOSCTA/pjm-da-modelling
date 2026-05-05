"""Feature-domain plugins for like_day_model_knn_sunny.

Each domain returns a long-format frame:

  - Hourly domains:           (date, hour_ending, *feature_cols)
  - Daily-broadcast domains:  (date, *feature_cols)        — no hour_ending

``_shared.py`` joins hourly domains on ``(date, hour_ending)`` and
broadcasts daily domains across the 24 HEs of each date.

Loader-API note. Sunny's original ``forecast.py`` calls
``_safe_load_vintage(load_fn, cache_dir, vintage="lead1")`` against an
older loader API that accepted a ``vintage=`` keyword. The current
``common.data.loader`` either accepts ``lead_days=1`` (for
``load_load_forecast`` / ``load_outages_forecast_history``) or has no
vintage kwarg (for ``load_solar_forecast`` /
``load_meteologica_*_forecast``) and instead carries an ``as_of_date``
column the caller must filter on. ``_safe_load_lead1`` here adapts
between the two — same behavior, current API.
"""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from da_models.common.data import loader

logger = logging.getLogger(__name__)

RTO = "RTO"


# ── FeatureDomain ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class FeatureDomain:
    """A toggleable feature domain.

    ``pool_builder``: returns the full historical feature frame.
        - Hourly domain frames: long form ``(date, hour_ending, *cols)``.
        - Daily-broadcast frames: ``(date, *cols)``; ``_shared.py``
          replicates them across all 24 HEs of the date.

    ``query_builder``: returns the same shape for ``target_date`` —
    24 rows for hourly, 1 row for daily-broadcast.

    The schema (presence of ``hour_ending``) is the only signal
    ``_shared.py`` uses to distinguish hourly vs broadcast.
    """

    name: str
    description: str
    feature_groups: dict[str, list[str]]
    feature_group_weights: dict[str, float]
    pool_builder: Callable[[Path | None], pd.DataFrame]
    query_builder: Callable[[date, Path | None], pd.DataFrame]

    @property
    def feature_cols(self) -> list[str]:
        seen: list[str] = []
        for cols in self.feature_groups.values():
            for c in cols:
                if c not in seen:
                    seen.append(c)
        return seen


# ── Loader helpers (faithful to forecast.py:55-176) ────────────────────


def _safe_load(
    load_fn: Callable, cache_dir: Path | None, **kwargs
) -> pd.DataFrame | None:
    """Wrap a loader call with try/except — returns None on failure."""
    try:
        return load_fn(cache_dir=cache_dir, **kwargs)
    except Exception as exc:
        logger.warning("Optional loader %s failed: %s", load_fn.__name__, exc)
        return None


def _filter_lead1(df: pd.DataFrame | None) -> pd.DataFrame | None:
    """Filter rows to ``as_of_date == date - 1`` (lead-1 / DA-cutoff vintage).

    No-op when the frame is None/empty or lacks ``as_of_date``. Idempotent
    when the loader has already pre-filtered (``load_load_forecast``).
    """
    if df is None or len(df) == 0 or "as_of_date" not in df.columns:
        return df
    delta = (
        pd.to_datetime(df["date"], errors="coerce")
        - pd.to_datetime(df["as_of_date"], errors="coerce")
    ).dt.days
    return df[delta == 1].copy()


def _safe_load_lead1(load_fn: Callable, cache_dir: Path | None) -> pd.DataFrame | None:
    """Lead-1 vintage loader. Uses ``lead_days=1`` when the function
    accepts it; otherwise loads then filters ``as_of_date == date - 1``.

    Maps Sunny's ``_safe_load_vintage(..., vintage="lead1")`` onto the
    current loader API.
    """
    try:
        sig = inspect.signature(load_fn)
        if "lead_days" in sig.parameters:
            return _safe_load(load_fn, cache_dir, lead_days=1)
    except (TypeError, ValueError):
        pass
    df = _safe_load(load_fn, cache_dir)
    return _filter_lead1(df) if df is not None else None


def _to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s).dt.date


def _filter_region(df: pd.DataFrame, region: str = RTO) -> pd.DataFrame:
    """Restrict to a region when the column exists; otherwise pass through."""
    if df is None or len(df) == 0:
        return df
    if "region" in df.columns:
        return df[df["region"].astype(str) == region].copy()
    return df


def _fallback_fill(
    base: pd.DataFrame,
    target_col: str,
    source_df: pd.DataFrame | None,
    source_col: str,
    on: tuple[str, ...] = ("date", "hour_ending"),
) -> pd.DataFrame:
    """Fill nulls in ``base[target_col]`` from ``source_df[source_col]``
    joined on ``on``. Mirror of ``forecast.py:154-167``."""
    if source_df is None or len(source_df) == 0 or source_col not in source_df.columns:
        return base
    fb = source_df[list(on) + [source_col]].rename(
        columns={source_col: f"_fb_{target_col}"}
    )
    out = base.merge(fb, on=list(on), how="left")
    out[target_col] = out[target_col].fillna(out[f"_fb_{target_col}"])
    return out.drop(columns=[f"_fb_{target_col}"])


def _empty_long(cols: list[str]) -> pd.DataFrame:
    """Empty long-format frame with the given feature cols."""
    return pd.DataFrame(columns=["date", "hour_ending"] + cols)


def _empty_daily(cols: list[str]) -> pd.DataFrame:
    """Empty daily-broadcast frame."""
    return pd.DataFrame(columns=["date"] + cols)


# ── rto_load_scalar ────────────────────────────────────────────────────
# Pool prefers RT realized; falls back to lead-1 forecast where RT is
# missing. Query prefers lead-1 forecast; falls back to RT (so backtests
# on past dates still work). Faithful to forecast.py:215-230, 351-374.


def _build_rto_load_scalar_pool(cache_dir: Path | None) -> pd.DataFrame:
    df_rt = _safe_load(loader.load_load_rt, cache_dir)
    base = _empty_long(["load_mw_at_hour"])
    if df_rt is not None and len(df_rt) > 0:
        rt = _filter_region(df_rt)
        if "rt_load_mw" in rt.columns:
            base = rt[["date", "hour_ending", "rt_load_mw"]].rename(
                columns={"rt_load_mw": "load_mw_at_hour"}
            )

    df_fc = _safe_load_lead1(loader.load_load_forecast, cache_dir)
    if df_fc is not None and "forecast_load_mw" in df_fc.columns:
        fc = _filter_region(df_fc)
        if len(base) == 0:
            base = fc[["date", "hour_ending", "forecast_load_mw"]].rename(
                columns={"forecast_load_mw": "load_mw_at_hour"}
            )
        else:
            base = _fallback_fill(base, "load_mw_at_hour", fc, "forecast_load_mw")

    if len(base) == 0:
        return base
    base["date"] = _to_date(base["date"])
    base["hour_ending"] = pd.to_numeric(base["hour_ending"], errors="coerce").astype(
        "Int64"
    )
    base = base.dropna(subset=["date", "hour_ending"]).copy()
    base["hour_ending"] = base["hour_ending"].astype(int)
    return base.sort_values(["date", "hour_ending"]).reset_index(drop=True)


def _build_rto_load_scalar_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    df_fc = _safe_load_lead1(loader.load_load_forecast, cache_dir)
    if df_fc is not None and len(df_fc) > 0 and "forecast_load_mw" in df_fc.columns:
        fc = _filter_region(df_fc)
        fc = fc[_to_date(fc["date"]) == target_date]
        if len(fc) > 0:
            out = fc[["date", "hour_ending", "forecast_load_mw"]].rename(
                columns={"forecast_load_mw": "load_mw_at_hour"}
            )
            out["date"] = _to_date(out["date"])
            out["hour_ending"] = pd.to_numeric(
                out["hour_ending"], errors="coerce"
            ).astype(int)
            return out

    df_rt = _safe_load(loader.load_load_rt, cache_dir)
    if df_rt is not None and len(df_rt) > 0:
        rt = _filter_region(df_rt)
        rt = rt[_to_date(rt["date"]) == target_date]
        if len(rt) > 0 and "rt_load_mw" in rt.columns:
            out = rt[["date", "hour_ending", "rt_load_mw"]].rename(
                columns={"rt_load_mw": "load_mw_at_hour"}
            )
            out["date"] = _to_date(out["date"])
            out["hour_ending"] = pd.to_numeric(
                out["hour_ending"], errors="coerce"
            ).astype(int)
            return out
    return _empty_long(["load_mw_at_hour"])


RTO_LOAD_SCALAR = FeatureDomain(
    name="rto_load_scalar",
    description="RTO load scalar at target HE. Pool: RT→forecast fallback; query: forecast→RT fallback.",
    feature_groups={"load": ["load_mw_at_hour"]},
    feature_group_weights={"load": 1.0},
    pool_builder=_build_rto_load_scalar_pool,
    query_builder=_build_rto_load_scalar_query,
)


# ── temperature_scalar ─────────────────────────────────────────────────
# Pool: observed → forecast fallback. Query: forecast → observed fallback.


def _build_temperature_scalar_pool(cache_dir: Path | None) -> pd.DataFrame:
    df_obs = _safe_load(loader.load_weather_observed_hourly, cache_dir)
    if df_obs is None or len(df_obs) == 0:
        df_obs = _safe_load(loader.load_weather_hourly, cache_dir)

    base = _empty_long(["temp_at_hour"])
    if df_obs is not None and "temp" in df_obs.columns:
        base = df_obs[["date", "hour_ending", "temp"]].rename(
            columns={"temp": "temp_at_hour"}
        )

    df_fc = _safe_load(loader.load_weather_forecast_hourly, cache_dir)
    if df_fc is not None and "temp" in df_fc.columns:
        if len(base) == 0:
            base = df_fc[["date", "hour_ending", "temp"]].rename(
                columns={"temp": "temp_at_hour"}
            )
        else:
            base = _fallback_fill(base, "temp_at_hour", df_fc, "temp")

    if len(base) == 0:
        return base
    base["date"] = _to_date(base["date"])
    base["hour_ending"] = pd.to_numeric(base["hour_ending"], errors="coerce").astype(
        "Int64"
    )
    base = base.dropna(subset=["date", "hour_ending"]).copy()
    base["hour_ending"] = base["hour_ending"].astype(int)
    return base.sort_values(["date", "hour_ending"]).reset_index(drop=True)


def _build_temperature_scalar_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    """Forecast-first per-target-date fallback to observed.

    Sunny's original ``forecast.py:377-386`` only swaps to observed when
    the forecast frame is fully empty. The live weather forecast mart
    only carries future dates (~today onward), so for any backtest target
    date in the past the forecast frame is non-empty but lacks the
    target's rows — the original silently returned NaN temperatures.
    Here we additionally fall back to observed when the forecast frame
    has no rows for ``target_date``, which is what the pool already does
    and what makes backtests usable.
    """
    out_cols = [["date", "hour_ending", "temp"]]
    _ = out_cols

    def _slice(df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or "temp" not in df.columns or len(df) == 0:
            return _empty_long(["temp_at_hour"])
        sl = df[_to_date(df["date"]) == target_date]
        if len(sl) == 0:
            return _empty_long(["temp_at_hour"])
        out = sl[["date", "hour_ending", "temp"]].rename(
            columns={"temp": "temp_at_hour"}
        )
        out["date"] = _to_date(out["date"])
        out["hour_ending"] = pd.to_numeric(out["hour_ending"], errors="coerce").astype(
            int
        )
        return out

    df_fc = _safe_load(loader.load_weather_forecast_hourly, cache_dir)
    out = _slice(df_fc)
    if len(out) > 0:
        return out
    df_obs = _safe_load(loader.load_weather_observed_hourly, cache_dir)
    if df_obs is None or len(df_obs) == 0:
        df_obs = _safe_load(loader.load_weather_hourly, cache_dir)
    return _slice(df_obs)


TEMPERATURE_SCALAR = FeatureDomain(
    name="temperature_scalar",
    description="Hourly temperature scalar. Pool: observed→forecast fallback; query: forecast→observed fallback.",
    feature_groups={"temperature": ["temp_at_hour"]},
    feature_group_weights={"temperature": 1.0},
    pool_builder=_build_temperature_scalar_pool,
    query_builder=_build_temperature_scalar_query,
)


# ── solar_scalar / wind_scalar ─────────────────────────────────────────
# Both follow the same fallback chain: lead-1 PJM forecast → lead-1
# Meteologica → realized fuel_mix. Faithful to forecast.py:251-289 / 388-440.


def _prep_meteo(df: pd.DataFrame | None, value_col: str) -> pd.DataFrame | None:
    if df is None or value_col not in df.columns or len(df) == 0:
        return None
    out = _filter_region(df)
    return out[["date", "hour_ending", value_col]].drop_duplicates(
        subset=["date", "hour_ending"], keep="first"
    )


def _build_renewable_scalar_pool(
    cache_dir: Path | None,
    *,
    forecast_loader: Callable,
    meteo_loader: Callable,
    forecast_col: str,
    fuel_mix_col: str,
    output_col: str,
) -> pd.DataFrame:
    base = _empty_long([output_col])
    base[output_col] = pd.Series(dtype=float)

    df_fc = _safe_load_lead1(forecast_loader, cache_dir)
    if df_fc is not None and forecast_col in df_fc.columns:
        out = df_fc[["date", "hour_ending", forecast_col]].rename(
            columns={forecast_col: output_col}
        )
        base = out

    df_meteo = _prep_meteo(_safe_load_lead1(meteo_loader, cache_dir), forecast_col)
    if df_meteo is not None:
        if len(base) == 0:
            base = df_meteo.rename(columns={forecast_col: output_col})
        else:
            base = _fallback_fill(base, output_col, df_meteo, forecast_col)

    df_fm = _safe_load(loader.load_fuel_mix, cache_dir)
    if df_fm is not None and fuel_mix_col in df_fm.columns:
        if len(base) == 0:
            base = df_fm[["date", "hour_ending", fuel_mix_col]].rename(
                columns={fuel_mix_col: output_col}
            )
        else:
            base = _fallback_fill(base, output_col, df_fm, fuel_mix_col)

    if len(base) == 0:
        return base
    base["date"] = _to_date(base["date"])
    base["hour_ending"] = pd.to_numeric(base["hour_ending"], errors="coerce").astype(
        "Int64"
    )
    base = base.dropna(subset=["date", "hour_ending"]).copy()
    base["hour_ending"] = base["hour_ending"].astype(int)
    return base.sort_values(["date", "hour_ending"]).reset_index(drop=True)


def _build_renewable_scalar_query(
    target_date: date,
    cache_dir: Path | None,
    *,
    forecast_loader: Callable,
    meteo_loader: Callable,
    forecast_col: str,
    output_col: str,
) -> pd.DataFrame:
    df_fc = _safe_load_lead1(forecast_loader, cache_dir)
    base = _empty_long([output_col])
    if df_fc is not None and forecast_col in df_fc.columns:
        df = df_fc[_to_date(df_fc["date"]) == target_date]
        if len(df) > 0:
            base = df[["date", "hour_ending", forecast_col]].rename(
                columns={forecast_col: output_col}
            )

    df_meteo = _prep_meteo(_safe_load_lead1(meteo_loader, cache_dir), forecast_col)
    if df_meteo is not None:
        df_meteo = df_meteo[_to_date(df_meteo["date"]) == target_date]
        if len(df_meteo) > 0:
            renamed = df_meteo.rename(columns={forecast_col: output_col})
            if len(base) == 0:
                base = renamed
            else:
                base = _fallback_fill(base, output_col, renamed, output_col)

    if len(base) == 0:
        return base
    base["date"] = _to_date(base["date"])
    base["hour_ending"] = pd.to_numeric(base["hour_ending"], errors="coerce").astype(
        int
    )
    return base


def _build_solar_scalar_pool(cache_dir: Path | None) -> pd.DataFrame:
    return _build_renewable_scalar_pool(
        cache_dir,
        forecast_loader=loader.load_solar_forecast,
        meteo_loader=loader.load_meteologica_solar_forecast,
        forecast_col="solar_forecast",
        fuel_mix_col="solar",
        output_col="solar_at_hour",
    )


def _build_solar_scalar_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    return _build_renewable_scalar_query(
        target_date,
        cache_dir,
        forecast_loader=loader.load_solar_forecast,
        meteo_loader=loader.load_meteologica_solar_forecast,
        forecast_col="solar_forecast",
        output_col="solar_at_hour",
    )


def _build_wind_scalar_pool(cache_dir: Path | None) -> pd.DataFrame:
    return _build_renewable_scalar_pool(
        cache_dir,
        forecast_loader=loader.load_wind_forecast,
        meteo_loader=loader.load_meteologica_wind_forecast,
        forecast_col="wind_forecast",
        fuel_mix_col="wind",
        output_col="wind_at_hour",
    )


def _build_wind_scalar_query(target_date: date, cache_dir: Path | None) -> pd.DataFrame:
    return _build_renewable_scalar_query(
        target_date,
        cache_dir,
        forecast_loader=loader.load_wind_forecast,
        meteo_loader=loader.load_meteologica_wind_forecast,
        forecast_col="wind_forecast",
        output_col="wind_at_hour",
    )


SOLAR_SCALAR = FeatureDomain(
    name="solar_scalar",
    description="Hourly solar scalar. Lead-1 PJM forecast → lead-1 Meteologica → fuel_mix realized.",
    feature_groups={"solar": ["solar_at_hour"]},
    feature_group_weights={"solar": 1.0},
    pool_builder=_build_solar_scalar_pool,
    query_builder=_build_solar_scalar_query,
)

WIND_SCALAR = FeatureDomain(
    name="wind_scalar",
    description="Hourly wind scalar. Lead-1 PJM forecast → lead-1 Meteologica → fuel_mix realized.",
    feature_groups={"wind": ["wind_at_hour"]},
    feature_group_weights={"wind": 1.0},
    pool_builder=_build_wind_scalar_pool,
    query_builder=_build_wind_scalar_query,
)


# ── load_ramps_scalar ──────────────────────────────────────────────────
# 1h and 3h load deltas. Wrap across day boundary: HE1's 1h ramp uses
# the prior date's HE24. Faithful to forecast.py:124-149 (pool) and
# 472-496 (query — prepends previous-day RT load before computing).


def _add_load_ramps(
    df: pd.DataFrame, source_col: str = "load_mw_at_hour"
) -> pd.DataFrame:
    if source_col not in df.columns:
        df["load_ramp_1h_at_hour"] = np.nan
        df["load_ramp_3h_at_hour"] = np.nan
        return df
    out = df.sort_values(["date", "hour_ending"]).reset_index(drop=True).copy()
    src = out[source_col].astype(float).to_numpy()
    shift1 = np.concatenate(([np.nan], src[:-1]))
    shift3 = np.concatenate(([np.nan, np.nan, np.nan], src[:-3]))
    out["load_ramp_1h_at_hour"] = src - shift1
    out["load_ramp_3h_at_hour"] = src - shift3
    return out


def _build_load_ramps_scalar_pool(cache_dir: Path | None) -> pd.DataFrame:
    """Reuse rto_load_scalar's pool then derive ramps over the sorted timeline."""
    base = _build_rto_load_scalar_pool(cache_dir)
    if len(base) == 0:
        return _empty_long(["load_ramp_1h_at_hour", "load_ramp_3h_at_hour"])
    out = _add_load_ramps(base[["date", "hour_ending", "load_mw_at_hour"]])
    return out[
        ["date", "hour_ending", "load_ramp_1h_at_hour", "load_ramp_3h_at_hour"]
    ].reset_index(drop=True)


def _build_load_ramps_scalar_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    """Compute target-date ramps with previous-day RT load prepended.

    Mirrors forecast.py:472-496: build the query's load row, prepend
    yesterday's RT load row, sort, compute diffs over the combined
    timeline, then drop the prepended rows so HE1's 1h ramp correctly
    references yesterday's HE24.
    """
    target_load = _build_rto_load_scalar_query(target_date, cache_dir)
    if len(target_load) == 0:
        return _empty_long(["load_ramp_1h_at_hour", "load_ramp_3h_at_hour"])

    prev_date = pd.Timestamp(target_date) - pd.Timedelta(days=1)
    prev_date_d = prev_date.date()

    df_rt = _safe_load(loader.load_load_rt, cache_dir)
    prev_load = pd.DataFrame(columns=["date", "hour_ending", "load_mw_at_hour"])
    if df_rt is not None and len(df_rt) > 0:
        rt = _filter_region(df_rt)
        rt = rt[_to_date(rt["date"]) == prev_date_d]
        if len(rt) > 0 and "rt_load_mw" in rt.columns:
            prev_load = rt[["date", "hour_ending", "rt_load_mw"]].rename(
                columns={"rt_load_mw": "load_mw_at_hour"}
            )
            prev_load["date"] = _to_date(prev_load["date"])
            prev_load["hour_ending"] = pd.to_numeric(prev_load["hour_ending"]).astype(
                int
            )

    if len(prev_load) > 0:
        combined = pd.concat(
            [prev_load.assign(__keep=False), target_load.assign(__keep=True)],
            ignore_index=True,
        )
        combined = combined.sort_values(["date", "hour_ending"]).reset_index(drop=True)
        combined = _add_load_ramps(combined)
        out = combined.loc[
            combined["__keep"],
            ["date", "hour_ending", "load_ramp_1h_at_hour", "load_ramp_3h_at_hour"],
        ].reset_index(drop=True)
    else:
        # No previous-day load — HE1's ramp will be NaN, which is correct.
        out = _add_load_ramps(target_load.copy())
        out = out[
            ["date", "hour_ending", "load_ramp_1h_at_hour", "load_ramp_3h_at_hour"]
        ]

    out["date"] = _to_date(out["date"])
    out["hour_ending"] = pd.to_numeric(out["hour_ending"]).astype(int)
    return out


LOAD_RAMPS_SCALAR = FeatureDomain(
    name="load_ramps_scalar",
    description="1h and 3h load ramps at target HE; wraps across day boundary via prepended prev-day load.",
    feature_groups={
        "load_ramps": ["load_ramp_1h_at_hour", "load_ramp_3h_at_hour"],
    },
    feature_group_weights={"load_ramps": 1.0},
    pool_builder=_build_load_ramps_scalar_pool,
    query_builder=_build_load_ramps_scalar_query,
)


# ── rto_net_load_scalar (faithful to Sunny's derivation) ───────────────
# Sunny's _add_net_load: net_load = load - solar.fillna(0) - wind.fillna(0).
# Treats missing renewables as zero so net_load stays computable when a
# forecast is missing (errs slightly toward over-counting net load).
# Identity-breaking by design — DO NOT migrate to the unified-coalescer
# path here (that's the sibling like_day_model_knn behavior).


def _build_rto_net_load_scalar_pool(cache_dir: Path | None) -> pd.DataFrame:
    load = _build_rto_load_scalar_pool(cache_dir)
    solar = _build_solar_scalar_pool(cache_dir)
    wind = _build_wind_scalar_pool(cache_dir)
    if len(load) == 0:
        return _empty_long(["net_load_at_hour"])

    out = load.merge(solar, on=["date", "hour_ending"], how="left")
    out = out.merge(wind, on=["date", "hour_ending"], how="left")
    s = out.get("solar_at_hour", pd.Series(np.nan, index=out.index))
    w = out.get("wind_at_hour", pd.Series(np.nan, index=out.index))
    out["net_load_at_hour"] = (
        out["load_mw_at_hour"].astype(float)
        - s.fillna(0.0).astype(float)
        - w.fillna(0.0).astype(float)
    )
    out.loc[out["load_mw_at_hour"].isna(), "net_load_at_hour"] = np.nan
    return out[["date", "hour_ending", "net_load_at_hour"]].reset_index(drop=True)


def _build_rto_net_load_scalar_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    load = _build_rto_load_scalar_query(target_date, cache_dir)
    solar = _build_solar_scalar_query(target_date, cache_dir)
    wind = _build_wind_scalar_query(target_date, cache_dir)
    if len(load) == 0:
        return _empty_long(["net_load_at_hour"])

    out = load.merge(solar, on=["date", "hour_ending"], how="left")
    out = out.merge(wind, on=["date", "hour_ending"], how="left")
    s = out.get("solar_at_hour", pd.Series(np.nan, index=out.index))
    w = out.get("wind_at_hour", pd.Series(np.nan, index=out.index))
    out["net_load_at_hour"] = (
        out["load_mw_at_hour"].astype(float)
        - s.fillna(0.0).astype(float)
        - w.fillna(0.0).astype(float)
    )
    out.loc[out["load_mw_at_hour"].isna(), "net_load_at_hour"] = np.nan
    return out[["date", "hour_ending", "net_load_at_hour"]].reset_index(drop=True)


RTO_NET_LOAD_SCALAR = FeatureDomain(
    name="rto_net_load_scalar",
    description=(
        "Net load = load - solar.fillna(0) - wind.fillna(0). Faithful to "
        "Sunny's NaN-as-zero derivation; identity does NOT hold when "
        "renewables forecasts have gaps."
    ),
    feature_groups={"net_load": ["net_load_at_hour"]},
    feature_group_weights={"net_load": 1.0},
    pool_builder=_build_rto_net_load_scalar_pool,
    query_builder=_build_rto_net_load_scalar_query,
)


# ── outages_scalar (daily-broadcast) ───────────────────────────────────
# Single col: outage_total_mw. Pool: lead-1 forecast → actuals fallback.
# Query: lead-1 forecast only. Faithful to forecast.py:305-324, 451-462.


def _build_outages_scalar_pool(cache_dir: Path | None) -> pd.DataFrame:
    base = _empty_daily(["outage_total_mw"])
    df_fc = _safe_load(loader.load_outages_forecast_history, cache_dir, lead_days=1)
    if df_fc is not None and len(df_fc) > 0:
        of = _filter_region(df_fc)
        date_col = "forecast_date" if "forecast_date" in of.columns else "date"
        of = of[[date_col, "total_outages_mw"]].rename(columns={date_col: "date"})
        of = of.drop_duplicates(subset=["date"], keep="first")
        of["date"] = _to_date(of["date"])
        of = of.rename(columns={"total_outages_mw": "outage_total_mw"})
        base = of[["date", "outage_total_mw"]]

    df_actual = _safe_load(loader.load_outages_actual, cache_dir)
    if df_actual is not None and len(df_actual) > 0:
        oa = _filter_region(df_actual)
        if "total_outages_mw" in oa.columns:
            oa = oa[["date", "total_outages_mw"]].rename(
                columns={"total_outages_mw": "_outage_actual"}
            )
            oa["date"] = _to_date(oa["date"])
            base = base.merge(oa, on="date", how="outer")
            if "outage_total_mw" not in base.columns:
                base["outage_total_mw"] = np.nan
            base["outage_total_mw"] = base["outage_total_mw"].fillna(
                base["_outage_actual"]
            )
            base = base.drop(columns=["_outage_actual"])

    return base.sort_values("date").reset_index(drop=True)[["date", "outage_total_mw"]]


def _build_outages_scalar_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    df_fc = _safe_load(loader.load_outages_forecast_history, cache_dir, lead_days=1)
    if df_fc is None or len(df_fc) == 0:
        return pd.DataFrame([{"date": target_date, "outage_total_mw": np.nan}])
    of = _filter_region(df_fc)
    date_col = "forecast_date" if "forecast_date" in of.columns else "date"
    of = of.copy()
    of["date"] = _to_date(of[date_col])
    of = of[of["date"] == target_date]
    if len(of) == 0:
        return pd.DataFrame([{"date": target_date, "outage_total_mw": np.nan}])
    val = pd.to_numeric(of.iloc[0].get("total_outages_mw"), errors="coerce")
    return pd.DataFrame(
        [
            {
                "date": target_date,
                "outage_total_mw": float(val) if pd.notna(val) else np.nan,
            }
        ]
    )


OUTAGES_SCALAR = FeatureDomain(
    name="outages_scalar",
    description="Single-col daily outage total (MW). Daily-broadcast across HEs.",
    feature_groups={"outage": ["outage_total_mw"]},
    feature_group_weights={"outage": 1.0},
    pool_builder=_build_outages_scalar_pool,
    query_builder=_build_outages_scalar_query,
)


# ── gas_scalar (daily-broadcast) ───────────────────────────────────────
# Daily mean of M3 across whatever hubs are available for the date.
# Faithful to forecast.py:292-300.


def _build_gas_scalar_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = _safe_load(loader.load_gas_prices_hourly, cache_dir)
    if df is None or len(df) == 0 or "gas_m3" not in df.columns:
        return _empty_daily(["gas_m3_daily_avg"])
    work = df[["date", "gas_m3"]].copy()
    work["date"] = _to_date(work["date"])
    work["gas_m3"] = pd.to_numeric(work["gas_m3"], errors="coerce")
    work = work.dropna(subset=["date", "gas_m3"])
    daily = work.groupby("date", as_index=False).agg(
        gas_m3_daily_avg=("gas_m3", "mean")
    )
    return (
        daily[["date", "gas_m3_daily_avg"]].sort_values("date").reset_index(drop=True)
    )


def _build_gas_scalar_query(target_date: date, cache_dir: Path | None) -> pd.DataFrame:
    daily = _build_gas_scalar_pool(cache_dir)
    out = daily[daily["date"] == target_date]
    if len(out) == 0:
        return pd.DataFrame([{"date": target_date, "gas_m3_daily_avg": np.nan}])
    return out.reset_index(drop=True)


GAS_SCALAR = FeatureDomain(
    name="gas_scalar",
    description="Daily mean M3 cash gas across available hubs. Daily-broadcast across HEs.",
    feature_groups={"gas": ["gas_m3_daily_avg"]},
    feature_group_weights={"gas": 1.0},
    pool_builder=_build_gas_scalar_pool,
    query_builder=_build_gas_scalar_query,
)


# ── Registry ───────────────────────────────────────────────────────────


DOMAIN_REGISTRY: dict[str, FeatureDomain] = {
    RTO_LOAD_SCALAR.name: RTO_LOAD_SCALAR,
    TEMPERATURE_SCALAR.name: TEMPERATURE_SCALAR,
    SOLAR_SCALAR.name: SOLAR_SCALAR,
    WIND_SCALAR.name: WIND_SCALAR,
    LOAD_RAMPS_SCALAR.name: LOAD_RAMPS_SCALAR,
    RTO_NET_LOAD_SCALAR.name: RTO_NET_LOAD_SCALAR,
    OUTAGES_SCALAR.name: OUTAGES_SCALAR,
    GAS_SCALAR.name: GAS_SCALAR,
}


# Daily-broadcast domains have no hour_ending column in their output;
# _shared.py uses this set to decide how to merge the domain's frame.
DAILY_BROADCAST_DOMAINS: frozenset[str] = frozenset(
    {OUTAGES_SCALAR.name, GAS_SCALAR.name}
)


def resolved_feature_groups(domain_names: tuple[str, ...]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for n in domain_names:
        out.update(DOMAIN_REGISTRY[n].feature_groups)
    return out


def resolved_raw_feature_group_weights(
    domain_names: tuple[str, ...],
) -> dict[str, float]:
    raw: dict[str, float] = {}
    for n in domain_names:
        raw.update(DOMAIN_REGISTRY[n].feature_group_weights)
    return raw


def resolved_feature_group_weights(domain_names: tuple[str, ...]) -> dict[str, float]:
    raw = resolved_raw_feature_group_weights(domain_names)
    total = sum(raw.values())
    if total <= 0:
        return raw
    return {k: v / total for k, v in raw.items()}


def all_feature_cols(domain_names: tuple[str, ...]) -> list[str]:
    seen: list[str] = []
    for n in domain_names:
        for c in DOMAIN_REGISTRY[n].feature_cols:
            if c not in seen:
                seen.append(c)
    return seen
