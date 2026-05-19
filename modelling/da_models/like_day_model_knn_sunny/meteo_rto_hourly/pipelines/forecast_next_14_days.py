"""Next-14-days meteo_rto_hourly Sunny-KNN forecast strip.

Forecasts delivery dates D+1 .. D+``HORIZON_DAYS`` for the configured
hub in one pass: builds the historical KNN pool once, fetches one
Meteologica latest-vintage frame for all forward target dates at once,
forward-fills the daily-broadcast feeds (outages, gas) onto every
horizon day, then runs the KNN match per target date. Prints a
one-row-per-day forward strip; when ``PER_DAY_DETAIL`` is on, also
prints the full per-HE ``Quantile Bands`` table for each forecast day
(same layout as the single-day report).

Coverage caveats baked in:

  - Demand (load / solar / wind / net_load) comes from the latest
    Meteologica vintage, which typically spans ~D+1..D+14. Days past
    the vintage's horizon get NaN demand features; the strip flags
    those days with ``feat_ok=False``.
  - Temperature comes from the WSI hourly forecast
    (``load_weather_forecast_hourly``); the WSI mart typically reaches
    ~14 forward days. Dates past the WSI horizon leave temp NaN and
    flag feat_ok=False alongside any Meteologica-tail gap.
  - Outages: lead-1 forecast carried forward from the last known
    value (the feed usually only reaches D+1).
  - Gas: daily mean carried forward from the last historical day.

Vintage caveat: the *pool's* historical rows still use the lead-1
PJM / Meteologica / RT priority chain from the sibling pjm_rto_hourly
domains, so the analog-match space mixes "lead-1 like-day" history
with a "latest-vintage" target query. The asymmetry is documented;
late-lead numbers are indicative, not calibrated.

Research / standalone -- nothing here writes Postgres.

Usage::

    python -m da_models.like_day_model_knn_sunny.meteo_rto_hourly.pipelines.forecast_next_14_days
    python modelling/da_models/like_day_model_knn_sunny/meteo_rto_hourly/pipelines/forecast_next_14_days.py
"""

from __future__ import annotations

import sys
import uuid
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.like_day_model_knn_sunny import configs  # noqa: E402
from da_models.like_day_model_knn_sunny.meteo_rto_hourly.builder import (  # noqa: E402
    build_horizon_query_rows,
    build_pool,
    filter_pool_by_year_months,
)
from da_models.like_day_model_knn_sunny.pjm_rto_hourly import (  # noqa: E402
    forecast,
    metrics as metrics_mod,
    printers,
)

RUN_DATE: date | None = None  # None -> today; targets D+1..D+HORIZON_DAYS
HORIZON_DAYS: int = 14
# When set, clips the strip at this delivery date instead of using
# HORIZON_DAYS. Targets become D+1..END_DATE inclusive. Set to None to
# fall back to HORIZON_DAYS.
END_DATE: date | None = date(2026, 5, 22)
# When set, restricts the analog pool to dates whose (year, month) is in
# the map. Edit this to override the default season-window pool with a
# hand-picked analog universe -- e.g., when the default pool is missing
# precedent for the forecast's load/temp regime and pulls the wrong
# reference cluster. Set to None for default behavior. The example
# below pulls April 2026 + May/June 2024-2025: April from this year
# keeps the recent grid-state context (outages, gas, calendar
# adjacency), May/June from prior years adds genuine summer-peak-load
# precedent that the May 2026 window can't supply.
POOL_YEAR_MONTHS: dict[int, list[int]] | None = {
    2026: [4],
    2025: [5, 6],
    2024: [5, 6],
}
# When True (the package default), Saturday and Sunday targets inherit
# the day-type profile from ``configs.DAY_TYPE_SCENARIO_PROFILES``,
# which forces ``same_dow_group=True`` -- i.e., Sat targets only see
# Sat analogs, Sun targets only see Sun analogs. Set to False to let
# the engine pick analogs from every DOW (Sat/Sun included for
# weekday targets and vice versa). Weekday targets are unaffected:
# their profile is empty.
USE_DAY_TYPE_PROFILES: bool = False
MODEL_NAME: str = configs.METEO_RTO_HOURLY_SUNNY_SPEC.name
HUB: str = configs.HUB
LABEL_SOURCE: str = configs.LABEL_SOURCE
PER_DAY_DETAIL: bool = True

DEFAULT_QUANTILES: tuple[float, ...] = (
    0.01,
    0.05,
    0.10,
    0.25,
    0.375,
    0.50,
    0.625,
    0.75,
    0.90,
    0.95,
    0.99,
)
DISPLAY_QUANTILES: tuple[float, ...] = (0.25, 0.375, 0.50, 0.625, 0.75)

_DAY_ABBR = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_ONPEAK_HOURS = list(configs.ONPEAK_HOURS)
_OFFPEAK_HOURS = list(configs.OFFPEAK_HOURS)
_Q_LO: float = 0.10
_Q_HI: float = 0.90

# Feature columns the strip uses to decide whether a day's feature
# block is "complete" -- a day with all listed columns non-null on all
# 24 HEs sets ``feat_ok=True``; otherwise the day is partial-coverage.
# Temperature is included so days past the WSI forecast horizon flip
# feat_ok=False even when Meteologica demand still covers them.
_DEMAND_COLS: tuple[str, ...] = (
    "load_mw_at_hour",
    "solar_at_hour",
    "wind_at_hour",
    "net_load_at_hour",
    "temp_at_hour",
)


def _block_mean(by_he: dict[int, float], hours: list[int]) -> float:
    vals = [by_he[h] for h in hours if h in by_he and pd.notna(by_he[h])]
    return float(np.mean(vals)) if vals else float("nan")


def _naive_last_week(pool: pd.DataFrame, target_date: date) -> np.ndarray | None:
    """Same-day-last-week DA LMP profile, used as the rMAE denominator
    when actuals for ``target_date`` are present in the pool. Mirrors
    the single-day pipeline's helper."""
    last_week = target_date - timedelta(days=7)
    sub = pool[pool["date"] == last_week]
    if len(sub) == 0:
        return None
    by_he: dict[int, float] = {}
    for _, r in sub.iterrows():
        v = r.get("lmp")
        if pd.notna(v):
            by_he[int(r["hour_ending"])] = float(v)
    if len(by_he) < 12:
        return None
    return np.array([by_he.get(h, np.nan) for h in range(1, 25)], dtype=float)


def _features_complete(query: pd.DataFrame) -> bool:
    if len(query) < 24:
        return False
    for c in _DEMAND_COLS:
        if c not in query.columns or query[c].isna().any():
            return False
    return True


def run(
    run_date: date | None = RUN_DATE,
    horizon_days: int = HORIZON_DAYS,
    end_date: date | None = END_DATE,
    model_name: str = MODEL_NAME,
    hub: str = HUB,
    label_source: str = LABEL_SOURCE,
    per_day_detail: bool = PER_DAY_DETAIL,
    quantiles: tuple[float, ...] | list[float] | None = None,
    display_quantiles: tuple[float, ...] | list[float] | None = None,
    cache_dir: Path | None = None,
    pool: pd.DataFrame | None = None,
    pool_year_months: dict[int, list[int]] | None = POOL_YEAR_MONTHS,
    use_day_type_profiles: bool = USE_DAY_TYPE_PROFILES,
    quiet: bool = False,
) -> dict:
    """Run the next-N-days meteo_rto_hourly Sunny-KNN forecast.

    Returns a dict: ``run_date``, ``horizon_days``, ``hub``,
    ``model_name``, ``strip_table`` (one row per delivery date),
    ``forecasts_by_date`` ({date_iso: per-HE forecast frame}),
    ``bands_by_date`` ({date_iso: quantile-bands table}),
    ``analogs_by_date``, ``n_pool``, ``run_id``.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    if model_name not in configs.MODEL_REGISTRY:
        raise ValueError(
            f"model_name='{model_name}' not in MODEL_REGISTRY "
            f"{tuple(configs.MODEL_REGISTRY.keys())}"
        )

    resolved_run_date = run_date if run_date is not None else date.today()
    run_id = str(uuid.uuid4())
    quantiles_list = list(quantiles if quantiles is not None else DEFAULT_QUANTILES)
    display_q = list(
        display_quantiles if display_quantiles is not None else DISPLAY_QUANTILES
    )
    cache_dir = cache_dir or configs.CACHE_DIR

    if end_date is not None:
        effective_horizon = (end_date - resolved_run_date).days
        if effective_horizon < 1:
            raise ValueError(
                f"end_date {end_date} must be at least one day after "
                f"run_date {resolved_run_date}"
            )
    else:
        effective_horizon = int(horizon_days)
    target_dates = [
        resolved_run_date + timedelta(days=k)
        for k in range(1, effective_horizon + 1)
    ]

    spec = configs.MODEL_REGISTRY[model_name]

    if pool is None:
        pool = build_pool(
            hub=hub,
            label_source=label_source,
            cache_dir=cache_dir,
            spec=spec,
        )

    pool_full_size = len(pool)
    pool = filter_pool_by_year_months(pool, pool_year_months)

    query_frames = build_horizon_query_rows(
        target_dates, cache_dir=cache_dir, spec=spec
    )

    rows: list[dict] = []
    forecasts_by_date: dict[str, pd.DataFrame] = {}
    bands_by_date: dict[str, pd.DataFrame] = {}
    analogs_by_date: dict[str, pd.DataFrame] = {}
    queries_by_date: dict[str, pd.DataFrame] = {}
    output_tables_by_date: dict[str, pd.DataFrame] = {}
    results_by_date: dict[str, dict] = {}
    metrics_by_date: dict[str, dict] = {}

    for d in target_dates:
        query = query_frames.get(d)
        feat_ok = bool(query is not None and _features_complete(query))

        # Even a partial-coverage day gets a forecast attempt -- the engine
        # z-scores per feature group, and missing groups drop out gracefully.
        # The strip flags partial days via ``feat_ok`` / ``n_he`` so consumers
        # know which rows to trust.
        cfg = configs.KnnModelConfig(
            forecast_date=str(d),
            model_name=model_name,
            n_analogs=configs.DEFAULT_N_ANALOGS,
            season_window_days=configs.SEASON_WINDOW_DAYS,
            min_pool_size=configs.MIN_POOL_SIZE,
            recency_half_life_days=configs.RECENCY_HALF_LIFE_DAYS,
            label_source=label_source,
            quantiles=quantiles_list,
            hub=hub,
            use_day_type_profiles=use_day_type_profiles,
        )
        result = forecast.run_forecast(
            target_date=d,
            config=cfg,
            cache_dir=cache_dir,
            pool=pool,
            query=query,
        )
        analogs = result["analogs"]
        df_fc = forecast.hourly_forecast_from_hour_analogs(analogs, quantiles_list)
        qt = forecast.build_quantiles_table(d, df_fc, display_q, analogs=analogs)

        forecasts_by_date[d.isoformat()] = df_fc
        bands_by_date[d.isoformat()] = qt
        analogs_by_date[d.isoformat()] = analogs
        queries_by_date[d.isoformat()] = query if query is not None else pd.DataFrame()
        output_tables_by_date[d.isoformat()] = result["output_table"]
        results_by_date[d.isoformat()] = result

        per_day_metrics: dict = {}
        if result["has_actuals"] and len(df_fc) > 0:
            actuals_long = pool[pool["date"] == d]
            actuals_by_he = {
                int(r["hour_ending"]): float(r["lmp"])
                for _, r in actuals_long.iterrows()
                if pd.notna(r.get("lmp"))
            }
            merged = df_fc.copy()
            merged["actual_lmp"] = merged["hour_ending"].map(actuals_by_he)
            merged = merged.dropna(subset=["actual_lmp"])
            if len(merged) > 0:
                y_true = merged["actual_lmp"].to_numpy(dtype=float)
                naive_full = _naive_last_week(pool, d)
                y_naive = (
                    naive_full[merged["hour_ending"].astype(int).values - 1]
                    if naive_full is not None
                    else None
                )
                per_day_metrics = metrics_mod.evaluate_forecast(
                    y_true, merged, quantiles_list, y_naive=y_naive
                )
        metrics_by_date[d.isoformat()] = per_day_metrics

        if len(df_fc) > 0:
            he = df_fc["hour_ending"].astype(int)
            point = dict(zip(he, df_fc["point_forecast"].astype(float)))
            q_lo_col = f"q_{_Q_LO:.2f}"
            q_hi_col = f"q_{_Q_HI:.2f}"
            q_lo = (
                dict(zip(he, df_fc[q_lo_col].astype(float)))
                if q_lo_col in df_fc.columns
                else {}
            )
            q_hi = (
                dict(zip(he, df_fc[q_hi_col].astype(float)))
                if q_hi_col in df_fc.columns
                else {}
            )
        else:
            point, q_lo, q_hi = {}, {}, {}

        rows.append(
            {
                "target_date": d.isoformat(),
                "lead": (d - resolved_run_date).days,
                "dow": _DAY_ABBR[d.weekday()],
                "onpeak": _block_mean(point, _ONPEAK_HOURS),
                "offpeak": _block_mean(point, _OFFPEAK_HOURS),
                "flat": _block_mean(point, list(range(1, 25))),
                "p10_onpeak": _block_mean(q_lo, _ONPEAK_HOURS),
                "p90_onpeak": _block_mean(q_hi, _ONPEAK_HOURS),
                "n_he": int(sum(1 for v in point.values() if pd.notna(v))),
                "n_analogs": int(result.get("n_analogs_used", 0)),
                "features_complete": feat_ok,
            }
        )

    strip_table = pd.DataFrame(rows)

    if not quiet:
        bar = "=" * 120
        print(bar)
        print(
            f"SUNNY KNN -- NEXT {len(rows)} DAYS -- {hub} ($/MWh)  |  "
            f"{model_name}  |  run_date {resolved_run_date}"
        )
        print(bar)
        print(f"  Model              {model_name}")
        print(
            f"  Targets            {target_dates[0]} .. {target_dates[-1]}  "
            f"(leads 1..{effective_horizon})"
        )
        print("  Demand (D+1..end)  Meteologica latest vintage (RTO)")
        print("  Temperature        WSI hourly forecast (load_weather_forecast_hourly)")
        print(
            "  Forward-filled     outage_total_mw, gas_m3_daily_avg  "
            "(carried from last known value)"
        )
        if pool_year_months:
            ym_str = ", ".join(
                f"{y}=[{','.join(str(m) for m in sorted(ms))}]"
                for y, ms in sorted(pool_year_months.items())
            )
            print(
                f"  Pool filter        custom universe -> {ym_str}  "
                f"({len(pool):,}/{pool_full_size:,} rows kept)"
            )
        if not use_day_type_profiles:
            print(
                "  Day-type profiles  DISABLED  "
                "(Sat/Sun targets see all DOWs, not just matching DOW)"
            )
        partial = [r["target_date"] for r in rows if not r["features_complete"]]
        if partial:
            print(
                "  Partial-feature days (past Meteologica / WSI horizon): "
                + ", ".join(partial)
            )
        print()

        print("Forward Strip ($/MWh)")
        disp = strip_table.copy()
        for c in ("onpeak", "offpeak", "flat", "p10_onpeak", "p90_onpeak"):
            disp[c] = disp[c].map(lambda v: "" if pd.isna(v) else f"{v:>9.2f}")
        disp = disp.rename(
            columns={
                "target_date": "delivery_date",
                "onpeak": "OnPk",
                "offpeak": "OffPk",
                "flat": "Flat",
                "p10_onpeak": "P10_OnPk",
                "p90_onpeak": "P90_OnPk",
                "features_complete": "feat_ok",
            }
        )[
            [
                "delivery_date",
                "lead",
                "dow",
                "OnPk",
                "OffPk",
                "Flat",
                "P10_OnPk",
                "P90_OnPk",
                "n_he",
                "n_analogs",
                "feat_ok",
            ]
        ]
        print(disp.to_string(index=False))
        print()
        print(bar)
        print()

        if per_day_detail:
            for d in target_dates:
                row = strip_table.loc[strip_table["target_date"] == d.isoformat()]
                if len(row) == 0 or int(row["n_he"].iloc[0]) == 0:
                    continue
                print(bar)
                print(
                    f"SUNNY KNN FORECAST -- {hub} ($/MWh)  |  {d}  "
                    f"({model_name})"
                )
                print(bar)
                day_cfg = configs.KnnModelConfig(
                    forecast_date=str(d),
                    model_name=model_name,
                    n_analogs=configs.DEFAULT_N_ANALOGS,
                    season_window_days=configs.SEASON_WINDOW_DAYS,
                    min_pool_size=configs.MIN_POOL_SIZE,
                    recency_half_life_days=configs.RECENCY_HALF_LIFE_DAYS,
                    label_source=label_source,
                    quantiles=quantiles_list,
                    hub=hub,
                    use_day_type_profiles=use_day_type_profiles,
                )
                resolved_cfg, day_type = day_cfg.with_day_type_overrides(d)
                result = results_by_date[d.isoformat()]
                analogs = analogs_by_date[d.isoformat()]
                query = queries_by_date[d.isoformat()]

                printers.print_config(
                    resolved_cfg,
                    spec,
                    d,
                    day_type,
                    effective_weights=result.get("feature_weights"),
                )
                printers.print_pool_summary(
                    pool, analogs, resolved_cfg, d, day_type
                )
                printers.print_analog_features(analogs, pool, query, d, hub)
                day_metrics = metrics_by_date.get(d.isoformat()) or None
                printers.print_forecast(output_tables_by_date[d.isoformat()], day_metrics)
                printers.print_quantiles(bands_by_date[d.isoformat()])
                print(bar)
                print()

    return {
        "run_date": str(resolved_run_date),
        "horizon_days": int(effective_horizon),
        "hub": hub,
        "model_name": model_name,
        "strip_table": strip_table,
        "forecasts_by_date": forecasts_by_date,
        "bands_by_date": bands_by_date,
        "analogs_by_date": analogs_by_date,
        "n_pool": len(pool),
        "run_id": run_id,
    }


if __name__ == "__main__":
    run()
