"""Shared feature-panel machinery for the linear ARX family.

Each variant supplies its own per-HE *demand* block (PJM RTO supply-demand,
or Meteologica regional supply-demand) and hands it to ``assemble_panel``,
which merges in the feed-agnostic groups -- DA-LMP labels, weather, ICE
next-day gas, PJM outage forecast, daily load level, calendar, the
engineered curvature interactions, and (optionally) the backward-LMP
reference-day anchors -- then returns the long ``(date, hour_ending)``
panel + ordered feature column list.

Forward feeds are read at the DA-cutoff vintage (``LEAD_DAYS``); the
shared coalesced loaders make a single forecast-vs-RT decision per
``(region, date)`` so ``net_load = load - solar - wind`` holds row-wise.
Every optional feed degrades to a dropped feature group with a warning
rather than killing the run; the DA-LMP label feed and the variant's
demand block are the only hard requirements.
"""

from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.common.calendar import compute_calendar_row
from da_models.common.data import loader
from da_models.common.data.lmp_pool import (
    LMP_HOUR_COLUMNS,
    build_lmp_labels,
    load_lmp_da,
)
from da_models.linear_arx_da_price import configs as C

logger = logging.getLogger(__name__)

KEY_COLS: list[str] = ["date", "hour_ending"]
LABEL_COL: str = "lmp"
_OUTAGE_REGION: str = "RTO"  # PJM outage forecast is footprint-total


# ── Small coercion helpers ─────────────────────────────────────────────────
def coerce_date_col(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    df = df.copy()
    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df


def coerce_hour_col(df: pd.DataFrame, col: str = "hour_ending") -> pd.DataFrame:
    df = df.copy()
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[col])
    df[col] = df[col].astype(int)
    return df


def _hourly_grid(dates: list[date]) -> pd.DataFrame:
    return pd.DataFrame([(d, h) for d in dates for h in C.HOURS], columns=KEY_COLS)


def _reference_day(target: date, lag_default: int, lag_monday: int) -> date:
    lag = lag_monday if target.weekday() == 0 else lag_default  # Monday == 0
    return target - timedelta(days=lag)


# ── DA-LMP labels (wide -> long) ───────────────────────────────────────────
def load_label_panels(
    cache_dir: Path | None, hub: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (long ``date|hour_ending|lmp`` panel, wide ``lmp_h1..lmp_h24`` frame)."""
    wide = build_lmp_labels(load_lmp_da(cache_dir), hub)
    if wide.empty:
        raise RuntimeError(f"No DA LMP labels for hub {hub!r}.")
    long = wide.melt(
        id_vars=["date"],
        value_vars=LMP_HOUR_COLUMNS,
        var_name="he_col",
        value_name=LABEL_COL,
    )
    long["hour_ending"] = long["he_col"].str.removeprefix("lmp_h").astype(int)
    long = coerce_date_col(long.drop(columns=["he_col"]))
    return long[["date", "hour_ending", LABEL_COL]], wide


# ── Feed-agnostic per-HE groups ────────────────────────────────────────────
def _weather(cache_dir: Path | None) -> pd.DataFrame | None:
    try:
        df = loader.load_weather_coalesced(cache_dir=cache_dir)
    except Exception as exc:  # noqa: BLE001
        logger.warning("weather unavailable (%s); skipping group", exc)
        return None
    df = coerce_date_col(coerce_hour_col(df))
    keep = [c for c in ("temp", "wind_speed_mph", "cloud_cover_pct") if c in df.columns]
    return df[KEY_COLS + keep].groupby(KEY_COLS, as_index=False).mean(numeric_only=True)


def _gas(cache_dir: Path | None) -> pd.DataFrame | None:
    try:
        df = loader.load_gas_prices_hourly(cache_dir=cache_dir)
    except Exception as exc:  # noqa: BLE001
        logger.warning("gas prices unavailable (%s); skipping group", exc)
        return None
    df = coerce_date_col(coerce_hour_col(df))
    keep = [c for c in ("gas_m3", "gas_tz6", "gas_dom_south") if c in df.columns]
    return df[KEY_COLS + keep].groupby(KEY_COLS, as_index=False).mean(numeric_only=True)


# ── Feed-agnostic daily groups (one row per date, broadcast onto every HE) ──
def _outages_daily(cache_dir: Path | None) -> pd.DataFrame | None:
    try:
        df = loader.load_outages_forecast_history(
            cache_dir=cache_dir, lead_days=C.LEAD_DAYS
        )
        date_col = "forecast_date" if "forecast_date" in df.columns else "date"
    except Exception as exc_hist:  # noqa: BLE001
        logger.warning(
            "outages forecast history unavailable (%s); trying flat feed", exc_hist
        )
        try:
            df = loader.load_outages_forecast(cache_dir=cache_dir)
            date_col = "date"
            if "forecast_day_number" in df.columns:
                df = df[df["forecast_day_number"] == C.LEAD_DAYS]
        except Exception as exc:  # noqa: BLE001
            logger.warning("outages forecast unavailable (%s); skipping group", exc)
            return None
    if "region" in df.columns:
        df = df[df["region"].astype(str) == _OUTAGE_REGION]
    if df.empty or "total_outages_mw" not in df.columns:
        logger.warning("no RTO outage forecast rows; skipping group")
        return None
    out = df[[date_col, "total_outages_mw"]].rename(
        columns={date_col: "date", "total_outages_mw": "outages_total_mw"}
    )
    return (
        coerce_date_col(out).groupby("date", as_index=False)["outages_total_mw"].mean()
    )


def _daily_load_level(
    per_he_demand: pd.DataFrame, primary_load_col: str
) -> pd.DataFrame:
    g = per_he_demand.groupby("date")[primary_load_col]
    return pd.DataFrame(
        {
            "date": g.mean().index,
            f"{primary_load_col}_daily_avg": g.mean().to_numpy(),
            f"{primary_load_col}_daily_peak": g.max().to_numpy(),
        }
    )


def _daily_degree_days(weather: pd.DataFrame | None) -> pd.DataFrame | None:
    if weather is None or "temp" not in weather.columns:
        return None
    g = weather.groupby("date")["temp"].mean()
    avg = g.to_numpy()
    return pd.DataFrame(
        {
            "date": g.index,
            "cdd": np.maximum(avg - 65.0, 0.0),
            "hdd": np.maximum(65.0 - avg, 0.0),
        }
    )


def _calendar_daily(dates: list[date]) -> pd.DataFrame:
    rows: list[dict] = []
    for d in dates:
        cal = compute_calendar_row(d)
        angle = 2.0 * math.pi * (d.month - 1) / 12.0
        rows.append(
            {
                "date": d,
                "is_weekend": 1.0 if cal["is_weekend"] else 0.0,
                "is_nerc_holiday": 1.0 if cal["is_nerc_holiday"] else 0.0,
                "dow_sin": float(cal["dow_sin"]),
                "dow_cos": float(cal["dow_cos"]),
                "month_sin": math.sin(angle),
                "month_cos": math.cos(angle),
            }
        )
    return pd.DataFrame(rows)


def _backward_lmp_daily(label_wide: pd.DataFrame) -> pd.DataFrame:
    onpeak = [f"lmp_h{h}" for h in range(8, 24)]
    offpeak = [f"lmp_h{h}" for h in (list(range(1, 8)) + [24])]
    df = label_wide.copy()
    df["bwd_lmp_daily_avg"] = df[LMP_HOUR_COLUMNS].mean(axis=1)
    df["bwd_lmp_daily_min"] = df[LMP_HOUR_COLUMNS].min(axis=1)
    df["bwd_lmp_onpeak_avg"] = df[onpeak].mean(axis=1)
    df["bwd_lmp_offpeak_avg"] = df[offpeak].mean(axis=1)
    keep = [
        "date",
        "bwd_lmp_daily_avg",
        "bwd_lmp_daily_min",
        "bwd_lmp_onpeak_avg",
        "bwd_lmp_offpeak_avg",
    ]
    return df[keep].rename(columns={"date": "ref_date"})


# ── Assembly ───────────────────────────────────────────────────────────────
def _forward_fill_onto_targets(
    panel: pd.DataFrame, cols: tuple[str, ...], target_dates: set[date]
) -> pd.DataFrame:
    """For each ``col`` in ``cols``, carry the last known historical value
    forward (per hour-ending) onto any *target-date* row that is NaN.

    Used by the multi-day-horizon pipelines: the ICE next-day gas feed only
    prices ~D+1 and the D-1-vintage outage forecast only exists for D+1, so
    the D+2..D+N horizon rows for those columns are filled from the most
    recent available value. Historical rows are left untouched.
    """
    present = [c for c in cols if c in panel.columns]
    if not present:
        return panel
    out = panel.sort_values(["hour_ending", "date"]).reset_index(drop=True)
    on_target = out["date"].isin(target_dates)
    for col in present:
        filled = out.groupby("hour_ending")[col].ffill()
        need = on_target & out[col].isna()
        out.loc[need, col] = filled[need]
    return out


def assemble_panel(
    target_date: date,
    *,
    cache_dir: Path | None,
    hub: str,
    per_he_demand: pd.DataFrame,
    primary_load_col: str,
    primary_gas_col: str,
    target_required_cols: list[str],
    primary_net_load_col: str | None = None,
    extra_target_dates: tuple[date, ...] = (),
    forward_fill_target_cols: tuple[str, ...] = (),
    train_window_days: int = C.TRAIN_WINDOW_DAYS,
    include_backward_lmp: bool = False,
    backward_default_lag: int = 1,
    backward_monday_lag: int = 3,
) -> dict:
    """Build the training + target feature panel from a variant's demand block.

    ``per_he_demand`` -- long ``date | hour_ending | <demand cols...>`` frame
    (already coerced and named by the variant builder). ``primary_load_col``
    is the demand column used for the daily load aggregates and the
    ``load_sq`` / ``load_x_gas`` interactions. ``target_required_cols`` are
    the demand columns that must be present (non-NaN) for all 24 HEs of a
    target date for that day's forecast to be considered feasible.

    Single-day callers pass just ``target_date``. Multi-day-horizon callers
    add the rest of the horizon via ``extra_target_dates`` and (typically)
    name ``forward_fill_target_cols`` so the columns whose feeds run out
    before the horizon end -- outages, ICE next-day gas -- are carried
    forward from the last known value onto the late-horizon rows.

    Returns a dict: ``panel`` (long; target rows have NaN label),
    ``feature_cols`` (ordered), ``label_wide`` (for the d-7 baseline +
    actuals), ``target_dates`` (sorted list), ``has_target_features``
    (bool, for the primary ``target_date``), ``has_target_features_by_date``
    ({date: bool}), ``dropped_groups`` (list).
    """
    label_long, label_wide = load_label_panels(cache_dir, hub)
    demand = per_he_demand.groupby(KEY_COLS, as_index=False).mean(numeric_only=True)

    target_dates = sorted({target_date, *extra_target_dates})
    last_date = target_dates[-1]
    earliest = target_date - timedelta(days=train_window_days)
    cand = set(d for d in label_long["date"].unique() if earliest <= d <= last_date)
    cand |= set(d for d in demand["date"].unique() if earliest <= d <= last_date)
    cand |= set(target_dates)
    dates = sorted(cand)

    panel = _hourly_grid(dates)
    panel = panel.merge(label_long, on=KEY_COLS, how="left")
    panel = panel.merge(demand, on=KEY_COLS, how="left")

    dropped: list[str] = []
    weather_df = _weather(cache_dir)
    if weather_df is None:
        dropped.append("weather")
    else:
        panel = panel.merge(weather_df, on=KEY_COLS, how="left")
    gas_df = _gas(cache_dir)
    if gas_df is None:
        dropped.append("gas")
    else:
        panel = panel.merge(gas_df, on=KEY_COLS, how="left")

    daily_frames: list[pd.DataFrame] = [
        _daily_load_level(demand, primary_load_col),
        _calendar_daily(dates),
    ]
    outages = _outages_daily(cache_dir)
    if outages is None:
        dropped.append("outages")
    else:
        daily_frames.append(outages)
    dd = _daily_degree_days(weather_df)
    if dd is not None:
        daily_frames.append(dd)
    daily = daily_frames[0]
    for f in daily_frames[1:]:
        daily = daily.merge(f, on="date", how="outer")
    panel = panel.merge(daily, on="date", how="left")

    # Carry feeds that run out before the horizon end forward onto the late
    # target rows -- BEFORE the engineered interactions, so load_x_gas /
    # outage_sq inherit the filled values.
    if forward_fill_target_cols:
        panel = _forward_fill_onto_targets(
            panel, forward_fill_target_cols, set(target_dates)
        )

    # Engineered curvature / scarcity features. The supply curve is convex in
    # *residual* demand, so the net-load square/gas-interaction terms carry
    # most of the heat-event response (a +3 sigma move in load is a much
    # larger move in net_load^2); the net-load *hinges* (max(net_load-knot,0))
    # are exactly 0 on ordinary days and let the fit put a steep slope only in
    # the scarcity region; outage_x_load is the compound "high load AND high
    # outages" signal (cf. backward_vs_forward_looking.md). Both gross and net
    # forms are included -- the fit weights them.
    if primary_gas_col in panel.columns and primary_load_col in panel.columns:
        panel["load_x_gas"] = panel[primary_load_col] * panel[primary_gas_col] / 1.0e3
    if primary_load_col in panel.columns:
        panel["load_sq"] = (panel[primary_load_col] ** 2) / 1.0e6
    if primary_net_load_col and primary_net_load_col in panel.columns:
        nl = panel[primary_net_load_col]
        panel["net_load_sq"] = (nl**2) / 1.0e6
        if primary_gas_col in panel.columns:
            panel["net_load_x_gas"] = nl * panel[primary_gas_col] / 1.0e3
        for knot in C.NET_LOAD_HINGE_KNOTS_MW:
            panel[f"net_load_hinge_{int(knot) // 1000}"] = (nl - float(knot)).clip(
                lower=0.0
            ) / 1.0e3
    if "outages_total_mw" in panel.columns:
        panel["outage_sq"] = (panel["outages_total_mw"] ** 2) / 1.0e6
        if primary_load_col in panel.columns:
            panel["outage_x_load"] = (
                panel["outages_total_mw"] * panel[primary_load_col] / 1.0e6
            )

    if include_backward_lmp:
        bwd = _backward_lmp_daily(label_wide)
        panel["ref_date"] = panel["date"].map(
            lambda d: _reference_day(d, backward_default_lag, backward_monday_lag)
        )
        panel = panel.merge(bwd, on="ref_date", how="left").drop(columns=["ref_date"])
    # backward_lmp being off is a config choice, not a missing feed -- the
    # config banner reports it via the "Backward LMP: off" line, so don't
    # list it under dropped_groups (which means "parquet unavailable").

    non_features = set(KEY_COLS) | {LABEL_COL}
    feature_cols = [c for c in panel.columns if c not in non_features]
    for c in feature_cols:
        panel[c] = pd.to_numeric(panel[c], errors="coerce")

    req = [c for c in target_required_cols if c in panel.columns]
    by_date: dict[date, bool] = {}
    for d in target_dates:
        rows_d = panel[panel["date"] == d]
        by_date[d] = bool(
            not rows_d.empty
            and all(rows_d[c].notna().sum() >= len(C.HOURS) for c in req)
        )

    return {
        "panel": panel.sort_values(KEY_COLS).reset_index(drop=True),
        "feature_cols": feature_cols,
        "label_wide": label_wide,
        "target_date": target_date,
        "target_dates": target_dates,
        "hub": hub,
        "has_target_features": by_date.get(target_date, False),
        "has_target_features_by_date": by_date,
        "dropped_groups": dropped,
    }
