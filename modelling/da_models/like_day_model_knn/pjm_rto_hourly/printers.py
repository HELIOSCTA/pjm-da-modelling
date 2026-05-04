"""Terminal printers for the pjm_rto_hourly forecast.

Mirrors the ``_print_*`` helpers in
helioscta-pjm-da/backend/src/like_day_forecast/pipelines/forecast.py so a
side-by-side run produces visually-comparable terminal output.

Adapted to the per-hour KNN engine's output shape: analogs are emitted
per (hour_ending, rank) pair, so the analogs table aggregates per-date
(n_hours appeared, mean distance, summed weight).
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable

import numpy as np
import pandas as pd
from colorama import Fore, Style, init as colorama_init
from tabulate import tabulate

from da_models.like_day_model_knn import configs
from da_models.like_day_model_knn.configs import KnnModelConfig, ModelSpec

colorama_init()
_HL_FORECAST = Style.BRIGHT + Fore.RED
_HL_QUARTILE = Fore.CYAN          # P25 / P75
_HL_INNER = Fore.YELLOW           # P37.5 / P62.5
_RS = Style.RESET_ALL
_ROW_STYLES: dict[str, str] = {
    "Forecast": _HL_FORECAST,
    "P25": _HL_QUARTILE, "P75": _HL_QUARTILE,
    "P37.5": _HL_INNER, "P62.5": _HL_INNER,
}

DAY_ABBR: dict[int, str] = {
    0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun",
}


def quantile_label(q: float) -> str:
    """Format quantile label (P25, P37.5, P90, ...)."""
    q_pct = q * 100
    if float(q_pct).is_integer():
        return f"P{int(q_pct):02d}"
    return f"P{q_pct:.1f}".rstrip("0").rstrip(".")


def print_config(
    config: KnnModelConfig,
    spec: ModelSpec,
    target_date: date,
    day_type: str,
) -> None:
    """Forecast configuration block (90-char banner)."""
    target_dow = DAY_ABBR[target_date.weekday()]
    weights = spec.feature_group_weights

    window = config.season_window_days
    win_start = target_date - timedelta(days=window)
    win_end = target_date + timedelta(days=window)

    print("\n" + "=" * 90)
    print("  FORECAST CONFIGURATION")
    print("=" * 90)

    print(f"\n  Target        {target_date} ({target_dow})")
    print(f"  Day-type      {day_type}")
    print(f"  Hub           {config.hub}")
    print(f"  Spec          {spec.name}")
    print(f"  Description   {spec.description}")

    half_life = config.recency_half_life_years
    if half_life is not None and float(half_life) > 0:
        weight_method = f"inverse_distance + age_decay (half-life={float(half_life):g}y)"
    else:
        weight_method = "inverse_distance"

    print(f"\n  --- Analog Selection {'-' * 28}")
    print(f"  N analogs          {config.n_analogs}")
    print(f"  Weight method      {weight_method}")
    print(f"  flt_radius         {spec.flt_radius}")

    print(f"\n  --- Pre-Filtering {'-' * 30}")
    print(
        f"  Season window      +/-{window}d  "
        f"({win_start.strftime('%b %d')} - {win_end.strftime('%b %d')})"
    )
    print(f"  DOW group filter   {config.same_dow_group}")
    print(f"  Exclude holidays   {config.exclude_holidays}")
    if config.exclude_dates:
        print(f"  Exclude dates      {', '.join(config.exclude_dates)}")
    print(f"  Min pool size      {config.min_pool_size}")

    print(f"\n  --- Recency {'-' * 36}")
    print(f"  Max age years      {config.max_age_years}")
    print(f"  Half-life years    {config.recency_half_life_years}")

    active = {k: v for k, v in sorted(weights.items()) if v > 0}
    disabled = [k for k, v in sorted(weights.items()) if v == 0]

    print(f"\n  --- Feature Group Weights (renormalized) {'-' * 8}")
    for name, w in sorted(active.items(), key=lambda x: -x[1]):
        bar = "#" * int(w * 40)
        print(f"  {name:<32} {w:>6.3f}  {bar}")

    if disabled:
        print(f"\n  --- Disabled Groups {'-' * 28}")
        print(f"  {', '.join(disabled)}")

    print("\n" + "=" * 90)


def print_analogs(analogs: pd.DataFrame, target_date: date, hub: str) -> None:
    """Top analog days table.

    The pjm_rto_hourly engine emits one analog row per (hour_ending, rank);
    we aggregate to per-date for display: count of HEs the date appears
    in, summed weight, mean distance.
    """
    target_dow = DAY_ABBR[target_date.weekday()]
    print("\n" + "=" * 90)
    print("  LIKE-DAY ANALOG DAYS (aggregated across HEs)")
    print(f"  Forecast: {target_date} ({target_dow})  |  Hub: {hub}")
    print("=" * 90)

    if analogs is None or len(analogs) == 0:
        print("\n  (no analogs returned)")
        return

    by_date = (
        analogs.groupby("date", as_index=False)
        .agg(
            n_hours=("hour_ending", "nunique"),
            mean_distance=("distance", "mean"),
            summed_weight=("weight", "sum"),
        )
        .sort_values("summed_weight", ascending=False)
        .reset_index(drop=True)
    )

    n_show = min(len(by_date), 20)
    display = by_date.head(n_show).copy()
    display.insert(0, "rank", range(1, len(display) + 1))
    display["date"] = pd.to_datetime(display["date"]).dt.strftime("%a %b-%d %Y")
    display["mean_distance"] = display["mean_distance"].map("{:.4f}".format)
    display["summed_weight"] = display["summed_weight"].map("{:.4f}".format)

    print()
    print(tabulate(display, headers="keys", tablefmt="simple", showindex=False))
    total_w = float(by_date["summed_weight"].sum())
    top5_w = float(by_date.head(5)["summed_weight"].sum())
    print(
        f"\n  Total unique dates: {len(by_date)} | "
        f"Total weight: {total_w:.2f} | "
        f"Top-5 date weight: {top5_w / total_w:.2%} | "
        f"Distance range: {analogs['distance'].min():.4f} — {analogs['distance'].max():.4f}"
    )


def print_forecast(table: pd.DataFrame, metrics: dict | None) -> None:
    """Actual / Forecast / Error table (120-char banner) + metrics block."""
    print("\n" + "=" * 120)
    print("  DA LMP LIKE-DAY FORECAST — Western Hub ($/MWh)")
    print("=" * 120)

    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    for _, row in table.iterrows():
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row.get(f"HE{h}")
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        for col in ("OnPeak", "OffPeak", "Flat"):
            val = row.get(col)
            line += f" {val:>7.2f}" if pd.notna(val) else f" {'':>7}"
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        print(line)

    print("-" * len(header))

    if metrics:
        if {"mae", "rmse", "mape"}.issubset(metrics.keys()):
            print(
                f"  MAE: ${metrics['mae']:.2f}/MWh  |  "
                f"RMSE: ${metrics['rmse']:.2f}/MWh  |  "
                f"MAPE: {metrics['mape']:.1f}%"
            )
        if "rmae" in metrics:
            verdict = "better" if metrics["rmae"] < 1 else "worse"
            print(
                f"  rMAE vs naive (last week): {metrics['rmae']:.3f} "
                f"({verdict} than naive)"
            )
        cov_parts: list[str] = []
        for label, key in (("80%PI", "coverage_80pct"),
                           ("90%PI", "coverage_90pct"),
                           ("98%PI", "coverage_98pct")):
            if metrics.get(key) is not None:
                cov_parts.append(f"{label}={metrics[key]:.0%}")
        if cov_parts:
            print(f"  Coverage: {' | '.join(cov_parts)}")
        if metrics.get("sharpness_90pct") is not None:
            print(f"  Sharpness (90%PI width): ${metrics['sharpness_90pct']:.2f}/MWh")
        if "mean_pinball" in metrics:
            print(f"  Mean Pinball Loss: {metrics['mean_pinball']:.4f}")
        if "crps" in metrics:
            print(f"  CRPS: {metrics['crps']:.4f}")

    print("=" * 120 + "\n")


def print_quantiles(table: pd.DataFrame) -> None:
    """Quantile bands table (P25 / P37.5 / P50 / Forecast / P62.5 / P75)."""
    print("  Quantile Bands ($/MWh)")
    print("-" * 100)

    header = f"{'Date':<12} {'Band':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))

    for _, row in table.iterrows():
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row.get(f"HE{h}")
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        for col in ("OnPeak", "OffPeak", "Flat"):
            val = row.get(col)
            line += f" {val:>7.2f}" if pd.notna(val) else f" {'':>7}"
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        print(line)

    print("-" * len(header) + "\n")
