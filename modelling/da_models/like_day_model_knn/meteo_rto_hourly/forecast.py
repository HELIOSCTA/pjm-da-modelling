"""Hourly forecast aggregation for meteo_rto_hourly - per-hour analogs.

Mirror of ``pjm_rto_hourly/forecast.py``. The aggregation logic is
data-source-agnostic (operates on the analog DataFrame and pool LMP
labels), so this is a verbatim copy to keep meteo_rto_hourly standing
on its own without a sibling import.

Analogs are per-(date, hour) tuples, so this module groups by hour_ending
and computes weighted averages within each hour's own ensemble. The
OnPeak/OffPeak/Flat aggregate quantile bands use **per-date joint
sampling** rather than a naive mean of per-HE quantiles: each candidate
analog date contributes its real historical window mean to the
aggregate distribution, weighted by its summed inverse-distance weight
across the window's hours.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from da_models.common.configs import HOURS
from da_models.common.forecast.output import add_summary_cols

_ONPEAK_HOURS: tuple[int, ...] = tuple(range(8, 24))  # HE8..HE23
_OFFPEAK_HOURS: tuple[int, ...] = tuple(list(range(1, 8)) + [24])  # HE1..HE7, HE24


def weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    idx = np.argsort(values)
    v = values[idx]
    w = weights[idx]
    cdf = np.cumsum(w)
    cdf = cdf / cdf[-1]
    return float(np.interp(q, cdf, v))


def hourly_forecast_from_hour_analogs(
    analogs: pd.DataFrame,
    quantiles: list[float],
) -> pd.DataFrame:
    """Aggregate per-(hour, rank) analog tuples into a 24-hour forecast.

    Expects ``analogs`` with columns: hour_ending, weight, lmp.
    Group by hour_ending and produce a weighted point + quantiles per HE.
    """
    if len(analogs) == 0 or not {"hour_ending", "weight", "lmp"}.issubset(
        analogs.columns
    ):
        return pd.DataFrame()

    rows: list[dict] = []
    for h in HOURS:
        sub = analogs[analogs["hour_ending"] == h].dropna(subset=["lmp"])
        if len(sub) == 0:
            continue
        values = sub["lmp"].to_numpy(dtype=float)
        w = sub["weight"].to_numpy(dtype=float)
        if w.sum() <= 0:
            continue
        w = w / w.sum()
        row = {"hour_ending": h, "point_forecast": float(np.average(values, weights=w))}
        for q in quantiles:
            row[f"q_{q:.2f}"] = weighted_quantile(values, w, q)
        rows.append(row)
    return pd.DataFrame(rows)


def aggregate_quantile_bands_joint(
    analogs: pd.DataFrame,
    pool: pd.DataFrame,
    quantiles: list[float],
    hour_groups: dict[str, list[int]] | None = None,
) -> dict[str, dict[float, float]]:
    """Per-date joint quantile bands for OnPeak/OffPeak/Flat aggregates.

    Preserves within-day price comovement: each candidate analog date
    contributes its real historical window mean (from ``pool``) to the
    aggregate distribution, weighted by its summed inverse-distance
    weight across the window's hours.

    Window definitions:
      - OnPeak  : HE8..HE23
      - OffPeak : HE1..HE7, HE24
      - Flat    : HE1..HE24
    """
    if hour_groups is None:
        hour_groups = {
            "OnPeak": list(_ONPEAK_HOURS),
            "OffPeak": list(_OFFPEAK_HOURS),
            "Flat": list(HOURS),
        }
    nan_out = {label: {q: float("nan") for q in quantiles} for label in hour_groups}
    if analogs is None or len(analogs) == 0 or pool is None or len(pool) == 0:
        return nan_out

    out: dict[str, dict[float, float]] = {}
    for label, hours in hour_groups.items():
        sub = analogs[analogs["hour_ending"].isin(hours)]
        if len(sub) == 0:
            out[label] = {q: float("nan") for q in quantiles}
            continue
        date_w = sub.groupby("date")["weight"].sum()
        candidate_dates = date_w.index.tolist()

        pool_sub = pool[
            pool["date"].isin(candidate_dates) & pool["hour_ending"].isin(hours)
        ][["date", "hour_ending", "lmp"]].dropna(subset=["lmp"])
        if len(pool_sub) == 0:
            out[label] = {q: float("nan") for q in quantiles}
            continue
        profile = pool_sub.pivot(index="date", columns="hour_ending", values="lmp")
        full = profile.dropna(how="any")
        if len(full) == 0:
            out[label] = {q: float("nan") for q in quantiles}
            continue

        window_means = full.mean(axis=1).to_numpy(dtype=float)
        weights = date_w.loc[full.index].to_numpy(dtype=float)
        if weights.sum() <= 0:
            out[label] = {q: float("nan") for q in quantiles}
            continue
        weights = weights / weights.sum()
        out[label] = {q: weighted_quantile(window_means, weights, q) for q in quantiles}
    return out


def build_quantiles_table(
    target_date: date,
    df_forecast: pd.DataFrame,
    display_quantiles: list[float] = (0.25, 0.375, 0.50, 0.625, 0.75),
    analogs: pd.DataFrame | None = None,
    pool: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Pivot quantile bands into the same wide shape as build_output_table."""
    if len(df_forecast) == 0:
        cols = (
            ["Date", "Type"]
            + [f"HE{h}" for h in range(1, 25)]
            + ["OnPeak", "OffPeak", "Flat"]
        )
        return pd.DataFrame(columns=cols)

    rows: list[dict] = []
    for q in sorted(display_quantiles):
        col = f"q_{q:.2f}"
        if col not in df_forecast.columns:
            continue
        label = _quantile_label(q)
        row = {"Date": target_date, "Type": label}
        for _, r in df_forecast.iterrows():
            row[f"HE{int(r['hour_ending'])}"] = (
                float(r[col]) if pd.notna(r[col]) else None
            )
        rows.append(add_summary_cols(row))

    forecast_row = {"Date": target_date, "Type": "Forecast"}
    for _, r in df_forecast.iterrows():
        forecast_row[f"HE{int(r['hour_ending'])}"] = (
            float(r["point_forecast"]) if pd.notna(r.get("point_forecast")) else None
        )
    forecast_row = add_summary_cols(forecast_row)

    insert_at = next(
        (i for i, row in enumerate(rows) if row["Type"] == "P50"),
        len(rows) // 2,
    )
    rows.insert(insert_at + 1, forecast_row)

    if analogs is not None and pool is not None and len(analogs) > 0:
        joint = aggregate_quantile_bands_joint(
            analogs, pool, list(sorted(display_quantiles))
        )
        for row in rows:
            if row["Type"] == "Forecast":
                continue
            try:
                q_for_row = float(row["Type"][1:]) / 100.0
            except (TypeError, ValueError):
                continue
            for label in ("OnPeak", "OffPeak", "Flat"):
                v = joint.get(label, {}).get(q_for_row)
                if v is not None:
                    row[label] = v

    cols = (
        ["Date", "Type"]
        + [f"HE{h}" for h in range(1, 25)]
        + ["OnPeak", "OffPeak", "Flat"]
    )
    return pd.DataFrame(rows, columns=cols)


def _quantile_label(q: float) -> str:
    q_pct = q * 100
    if float(q_pct).is_integer():
        return f"P{int(q_pct):02d}"
    return f"P{q_pct:.1f}".rstrip("0").rstrip(".")
