"""Terminal printers for the pjm_rto_hourly forecast.

Mirrors the ``_print_*`` helpers in
helioscta-pjm-da/backend/src/like_day_forecast/pipelines/forecast.py so a
side-by-side run produces visually-comparable terminal output.

Adapted to the per-hour KNN engine's output shape: analogs are emitted
per (hour_ending, rank) pair, so the analogs table aggregates per-date
(n_hours appeared, mean distance, summed weight). The richer
``print_analog_features`` view (daily features + per-HE z-strips) is
ported from the sibling like_day_model_knn_sunny printer to keep the
two terminal reports visually identical.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
from tabulate import tabulate

from da_models.like_day_model_knn.configs import KnnModelConfig, ModelSpec
from utils.logging_utils import (
    Colors,
    print_divider,
    print_header,
    print_section,
    supports_color,
)

_COLOR_ON = supports_color()
_HL_FORECAST = (Colors.BOLD + Colors.BRIGHT_RED) if _COLOR_ON else ""
_HL_QUARTILE = Colors.CYAN if _COLOR_ON else ""
_HL_INNER = Colors.YELLOW if _COLOR_ON else ""
_HL_UP = Colors.BRIGHT_GREEN if _COLOR_ON else ""
_HL_DOWN = Colors.BRIGHT_RED if _COLOR_ON else ""
_RS = Colors.RESET if _COLOR_ON else ""
_ROW_STYLES: dict[str, str] = {
    "Forecast": _HL_FORECAST,
    "P25": _HL_QUARTILE,
    "P75": _HL_QUARTILE,
    "P37.5": _HL_INNER,
    "P62.5": _HL_INNER,
}

DAY_ABBR: dict[int, str] = {
    0: "Mon",
    1: "Tue",
    2: "Wed",
    3: "Thu",
    4: "Fri",
    5: "Sat",
    6: "Sun",
}

_FEATURE_KEYS: tuple[str, ...] = (
    "da_onpk",
    "load",
    "temp",
    "solar",
    "wind",
    "outages",
    "m3",
)


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

    print_header("FORECAST CONFIGURATION", "=", 90)

    print(f"\n  Target        {target_date} ({target_dow})")
    print(f"  Day-type      {day_type}")
    print(f"  Hub           {config.hub}")
    print(f"  Spec          {spec.name}")
    print(f"  Description   {spec.description}")

    half_life_days = float(getattr(config, "recency_half_life_days", 0.0) or 0.0)
    if half_life_days > 0:
        weight_method = (
            "inverse_distance (analog weights) + linear pre-selection age "
            f"penalty (half-life={half_life_days:g} days)"
        )
    else:
        weight_method = "inverse_distance"

    print_section("Analog Selection")
    print(f"  N analogs          {config.n_analogs}")
    print(f"  Weight method      {weight_method}")
    print(f"  flt_radius         {spec.flt_radius}")

    print_section("Pre-Filtering")
    print(
        f"  Season window      +/-{window}d  "
        f"({win_start.strftime('%b %d')} - {win_end.strftime('%b %d')})"
    )
    print(f"  DOW group filter   {config.same_dow_group}")
    print(
        f"  Weekend filter     same_weekend_group={config.same_weekend_group}  "
        f"for_weekends={config.same_weekend_group_for_weekends}"
    )
    print(f"  Exclude holidays   {config.exclude_holidays}")
    if config.exclude_dates:
        print(f"  Exclude dates      {', '.join(config.exclude_dates)}")
    print(f"  Min pool size      {config.min_pool_size}")

    print_section("Recency")
    print(f"  Max age years      {config.max_age_years}")
    print(f"  Half-life days     {half_life_days:g}")
    print(f"  Label source       {getattr(config, 'label_source', 'hub_lmp')}")

    from da_models.like_day_model_knn import configs as _configs_module
    from da_models.like_day_model_knn.domains import feature_group_weight_locations

    raw_weights = spec.raw_feature_group_weights
    active = {k: v for k, v in sorted(weights.items()) if v > 0}
    disabled = [k for k, v in sorted(weights.items()) if v == 0]
    raw_total = sum(raw_weights.get(k, 0.0) for k in active)
    locations = feature_group_weight_locations()

    print_section("Feature Group Weights")
    print(f"  Spec: {_configs_module.__file__}")
    loc_strs = {
        k: f"{locations[k][0]}:{locations[k][1]}" for k in active if k in locations
    }
    loc_w = max((len(s) for s in loc_strs.values()), default=0)
    bar_w = max((int(w * 40) for w in active.values()), default=0)
    print(
        f"  {'group':<32} {'raw':>6} {'norm':>6}  "
        f"{'bar':<{bar_w}}  {'defined at':<{loc_w}}"
    )
    for name, w in sorted(active.items(), key=lambda x: -x[1]):
        bar = "#" * int(w * 40)
        raw = raw_weights.get(name, 0.0)
        loc_str = loc_strs.get(name, "—")
        print(f"  {name:<32} {raw:>6.3f} {w:>6.3f}  {bar:<{bar_w}}  {loc_str:<{loc_w}}")
    print(f"  {'(sum)':<32} {raw_total:>6.3f} {1.0:>6.3f}")

    if disabled:
        print_section("Disabled Groups")
        print(f"  {', '.join(disabled)}")

    print()
    print_divider("=", 90, dim=False)


def print_pool_funnel(
    funnel,
    target_date: date,
    day_type: str,
    hub: str,
) -> None:
    """Render the candidate-pool funnel before distance computation."""
    target_dow = DAY_ABBR[target_date.weekday()]
    print_header("POOL FUNNEL", "=", 110)
    print(
        f"  Forecast: {target_date} ({target_dow})  |  Day-type: {day_type}  "
        f"|  Hub: {hub}"
    )
    print_divider("=", 110, dim=False)

    if funnel is None or not funnel.stages:
        print("\n  (no funnel records)")
        return

    raw = funnel.initial
    final = funnel.final

    print()
    print(
        f"  {'Stage':<5}  {'Filter':<24}  {'Detail':<46}  "
        f"{'Survives':>9}  {'Dropped':>9}  {'Cumul %':>8}"
    )
    print("  " + "─" * 108)
    for i, st in enumerate(funnel.stages):
        cum_pct = (st.survives / raw * 100.0) if raw > 0 else 0.0
        if st.relaxed:
            wd = st.would_survive if st.would_survive is not None else 0
            survives_str = (
                f"{_HL_DOWN}{st.survives:>9,}{_RS}"
                if _COLOR_ON
                else f"{st.survives:>9,}"
            )
            dropped_str = f"{'(relaxed)':>9}"
            detail_str = f"{st.detail}  → would yield {wd:,} (< min, kept full)"
        else:
            survives_str = f"{st.survives:>9,}"
            dropped_str = f"{-st.dropped:>+9,}" if st.dropped > 0 else f"{0:>9}"
            detail_str = st.detail
        print(
            f"  {i:<5}  {st.name:<24}  {detail_str[:46]:<46}  "
            f"{survives_str}  {dropped_str}  {cum_pct:>7.1f}%"
        )
    print("  " + "─" * 108)

    overall_drop = raw - final
    overall_pct = (final / raw * 100.0) if raw > 0 else 0.0
    final_color = f"{Colors.BOLD}{Colors.BRIGHT_GREEN}" if _COLOR_ON else ""
    print(
        f"  → {final_color}{final:,}{_RS} candidates feed find_twins  "
        f"|  {overall_drop:,} dropped overall ({overall_pct:.1f}% retained)"
    )

    print()
    print_divider("=", 110, dim=False)


# ── analog-features helpers (ported from like_day_model_knn_sunny) ────────


def _daily_features_long(pool: pd.DataFrame) -> pd.DataFrame:
    """Per-date daily features from a long-format pool. Indexed by date."""
    if pool is None or len(pool) == 0:
        return pd.DataFrame()
    grouped = pool.groupby("date", sort=True)
    onpk_mask = pool["hour_ending"].between(8, 23, inclusive="both")
    onpk = (
        pool[onpk_mask].groupby("date")["lmp"].mean()
        if "lmp" in pool.columns
        else pd.Series(dtype=float)
    )
    out = pd.DataFrame(
        {
            "da_onpk": onpk,
            "load": grouped["load_mw_at_hour"].max()
            if "load_mw_at_hour" in pool.columns
            else np.nan,
            "temp": grouped["temp_at_hour"].mean()
            if "temp_at_hour" in pool.columns
            else np.nan,
            "solar": grouped["solar_at_hour"].max()
            if "solar_at_hour" in pool.columns
            else np.nan,
            "wind": grouped["wind_at_hour"].max()
            if "wind_at_hour" in pool.columns
            else np.nan,
            "outages": grouped["outage_total_mw"].first()
            if "outage_total_mw" in pool.columns
            else np.nan,
            "m3": grouped["gas_m3_daily_avg"].first()
            if "gas_m3_daily_avg" in pool.columns
            else np.nan,
        }
    )
    return out


def _daily_features_from_query(query: pd.DataFrame) -> dict[str, float | None]:
    """Same daily features for the 24-row target query."""
    if query is None or len(query) == 0:
        return {k: None for k in _FEATURE_KEYS}

    def _max(col: str) -> float | None:
        if col not in query.columns:
            return None
        v = query[col].astype(float).dropna()
        return float(v.max()) if len(v) else None

    def _mean(col: str) -> float | None:
        if col not in query.columns:
            return None
        v = query[col].astype(float).dropna()
        return float(v.mean()) if len(v) else None

    def _scalar(col: str) -> float | None:
        if col not in query.columns:
            return None
        v = query[col].dropna()
        return float(v.iloc[0]) if len(v) else None

    return {
        "da_onpk": None,
        "load": _max("load_mw_at_hour"),
        "temp": _mean("temp_at_hour"),
        "solar": _max("solar_at_hour"),
        "wind": _max("wind_at_hour"),
        "outages": _scalar("outage_total_mw"),
        "m3": _scalar("gas_m3_daily_avg"),
    }


def _pool_feature_stds_long(pool: pd.DataFrame) -> dict[str, float]:
    daily = _daily_features_long(pool)
    out: dict[str, float] = {}
    for k in _FEATURE_KEYS:
        if k not in daily.columns:
            out[k] = 1.0
            continue
        s = float(np.nanstd(daily[k].to_numpy(dtype=float), ddof=0))
        out[k] = s if s > 0 and not np.isnan(s) else 1.0
    return out


def _scalar_feature_he_z(
    pool: pd.DataFrame,
    query: pd.DataFrame,
    feature_col: str,
) -> dict[date, list[float | None]]:
    """Per-HE z-distance for one scalar feature."""
    if (
        feature_col not in pool.columns
        or feature_col not in query.columns
        or len(query) == 0
    ):
        return {}

    out: dict[date, list[float | None]] = {}
    target_by_he = query.set_index("hour_ending")[feature_col].astype(float).to_dict()

    for h in range(1, 25):
        slice_pool = pool[pool["hour_ending"] == h]
        if len(slice_pool) == 0:
            continue
        vals = slice_pool[feature_col].astype(float).to_numpy()
        std = float(np.nanstd(vals)) if not np.all(np.isnan(vals)) else 1.0
        if std == 0 or np.isnan(std):
            std = 1.0
        target_v = target_by_he.get(h)
        if target_v is None or pd.isna(target_v):
            continue
        diffs = (vals - float(target_v)) / std
        for d, z in zip(slice_pool["date"].tolist(), diffs):
            arr = out.setdefault(d, [None] * 24)
            arr[h - 1] = None if np.isnan(z) else float(z)
    return out


def _shade_z(z: float | None) -> str:
    if z is None or (isinstance(z, float) and np.isnan(z)):
        return "·"
    a = abs(float(z))
    if a < 0.25:
        return "█"
    if a < 0.50:
        return "▓"
    if a < 1.00:
        return "▒"
    if a < 2.00:
        return "░"
    return "·"


def _he_strip_for_date(date_analogs: pd.DataFrame) -> tuple[str, int]:
    by_he: dict[int, int] = {
        int(r["hour_ending"]): int(r["rank"]) for _, r in date_analogs.iterrows()
    }
    chars: list[str] = []
    for h in range(1, 25):
        if h not in by_he:
            chars.append("·")
        else:
            r = by_he[h]
            chars.append("█" if r <= 5 else ("▓" if r <= 15 else "▒"))
    return "".join(chars), len(by_he)


def _fmt_num(
    v: float | None, width: int, decimals: int = 0, comma: bool = False
) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return f"{'-':>{width}}"
    spec = f">{width},.{decimals}f" if comma else f">{width}.{decimals}f"
    return f"{v:{spec}}"


def _fmt_diff(
    d: float | None,
    total_width: int,
    decimals: int = 0,
    comma: bool = False,
) -> str:
    if d is None or (isinstance(d, float) and pd.isna(d)):
        return " " * total_width
    spec = f"+,.{decimals}f" if comma else f"+.{decimals}f"
    body = f"({d:{spec}})".rjust(total_width)
    if d > 0:
        return f"{_HL_UP}{body}{_RS}"
    if d < 0:
        return f"{_HL_DOWN}{body}{_RS}"
    return body


def _fmt_sigma(z: float | None, width: int = 7) -> str:
    if z is None or (isinstance(z, float) and pd.isna(z)):
        return f"{'-':>{width}}"
    body = f"{z:+.2f}".rjust(width)
    if z > 0:
        return f"{_HL_UP}{body}{_RS}"
    if z < 0:
        return f"{_HL_DOWN}{body}{_RS}"
    return body


def print_analog_features(
    analogs: pd.DataFrame,
    pool: pd.DataFrame,
    query: pd.DataFrame,
    target_date: date,
    hub: str,
    n_show: int = 20,
) -> None:
    """Combined daily-features + engine-view table for the active config.

    Long-format pool: daily features are groupby(date) reducers (peak
    load/solar/wind, scalar outages/m3, mean LMP HE8-23). Per-feature
    sub-strips use scalar z per HE.
    """
    target_dow = DAY_ABBR[target_date.weekday()]
    print_header("LIKE-DAY ANALOGS - Daily Features + Engine View", "=", 230)
    print(f"  Forecast: {target_date} ({target_dow})  |  Hub: {hub}")
    print(
        "  Each cell: value  (raw diff)  sigma-gap   (GREEN = analog higher, RED = lower)   "
        "HE strip rank: full<=5  med<=15  light>15  dot=miss"
    )
    print(
        "  Per-feature sub-rows |z|-shading:  full<0.25  med<0.50  light<1.0  faint<2.0  dot>=2 or n/a"
    )
    print_divider("=", 230, dim=False)

    if analogs is None or len(analogs) == 0:
        print("\n  (no analogs returned)")
        return

    by_date = analogs.groupby("date", as_index=False).agg(
        summed_weight=("weight", "sum"),
        mean_distance=("distance", "mean"),
    )
    total_w = float(by_date["summed_weight"].sum())
    if total_w <= 0:
        print("\n  (zero total weight)")
        return
    by_date["w"] = by_date["summed_weight"] / total_w
    by_date = by_date.sort_values("w", ascending=False).reset_index(drop=True)

    daily_pool = _daily_features_long(pool)
    rows_features: list[dict] = []
    for idx, r in by_date.iterrows():
        d = r["date"]
        if d in daily_pool.index:
            feats = {
                k: float(daily_pool.loc[d, k])
                if pd.notna(daily_pool.loc[d, k])
                else None
                for k in _FEATURE_KEYS
            }
        else:
            feats = {k: None for k in _FEATURE_KEYS}
        feats["date"] = d
        feats["rank"] = int(idx) + 1
        feats["mean_distance"] = float(r["mean_distance"])
        feats["summed_weight"] = float(r["summed_weight"])
        feats["w"] = float(r["w"])
        rows_features.append(feats)

    avg: dict[str, float | None] = {}
    for k in _FEATURE_KEYS:
        wsum = 0.0
        wseen = 0.0
        for r in rows_features:
            if r[k] is not None:
                wsum += r[k] * r["w"]
                wseen += r["w"]
        avg[k] = (wsum / wseen) if wseen > 0 else None

    target_feats = _daily_features_from_query(query)
    if target_feats["da_onpk"] is None and target_date in daily_pool.index:
        v = daily_pool.loc[target_date, "da_onpk"]
        target_feats["da_onpk"] = float(v) if pd.notna(v) else None

    stds = _pool_feature_stds_long(pool)

    _COLS = (
        ("da_onpk", "DA OnPk", "($/MWh)", 8, 8, 2, False),
        ("load", "Load", "(MW)", 9, 9, 0, True),
        ("temp", "Temp", "(F)", 7, 7, 1, False),
        ("solar", "Solar", "(MW)", 9, 9, 0, True),
        ("wind", "Wind", "(MW)", 9, 9, 0, True),
        ("outages", "Outages", "(MW)", 9, 9, 0, True),
        ("m3", "M3", "($)", 6, 7, 2, False),
    )
    SIGMA_W = 6

    _PREFIX = f"  {'rank':>4} {'Like Date':<22} {'mean_d':>7} {'sum_w':>7} {'w':>6}"
    _PREFIX_UNITS = f"  {'':>4} {'':<22} {'':>7} {'':>7} {'':>6}"
    head_parts = [_PREFIX]
    unit_parts = [_PREFIX_UNITS]
    for _, label, units, vw, dw, _, _ in _COLS:
        cell_w = vw + 1 + dw + 1 + SIGMA_W
        head_parts.append(f"{label:^{cell_w}}")
        unit_parts.append(f"{units:^{cell_w}}")
    head_parts.append(f"{'HEs':>4}")
    unit_parts.append(f"{'/24':>4}")
    header = "  ".join(head_parts)
    units_row = "  ".join(unit_parts)
    sep = "-" * len(header)

    print()
    print(header)
    print(units_row)
    print(sep)

    def _fmt_row(
        rank_str: str,
        label: str,
        mean_d_str: str,
        sum_w_str: str,
        w_str: str,
        f: dict,
        target: dict | None,
        n_hours_str: str,
    ) -> str:
        parts = [
            f"  {rank_str:>4} {label:<22} {mean_d_str:>7} {sum_w_str:>7} {w_str:>6}"
        ]
        for key, _, _, vw, dw, decimals, comma in _COLS:
            val_str = _fmt_num(f[key], vw, decimals, comma=comma)
            if target is None:
                diff_str = " " * dw
                sigma_str = f"{'ref':>{SIGMA_W}}"
            elif f[key] is None or target[key] is None:
                diff_str = " " * dw
                sigma_str = f"{'-':>{SIGMA_W}}"
            else:
                diff = f[key] - target[key]
                diff_str = _fmt_diff(diff, dw, decimals, comma=comma)
                z = diff / stds[key]
                sigma_str = _fmt_sigma(z, SIGMA_W)
            parts.append(f"{val_str} {diff_str} {sigma_str}")
        parts.append(f"{n_hours_str:>4}")
        return "  ".join(parts)

    _SUB_PREFIXES: tuple[str, ...] = ("load", "temp", "solar", "wind")
    _SUB_TO_COL = {
        "load": "load_mw_at_hour",
        "temp": "temp_at_hour",
        "solar": "solar_at_hour",
        "wind": "wind_at_hour",
    }
    sub_z_by_feat: dict[str, dict] = {
        prefix: _scalar_feature_he_z(pool, query, _SUB_TO_COL[prefix])
        for prefix in _SUB_PREFIXES
    }

    def _print_sub_strips(d, feats: dict) -> None:
        parts = [_PREFIX_UNITS]
        sub_strips: dict[str, str] = {
            prefix: "".join(
                _shade_z(z) for z in sub_z_by_feat[prefix].get(d, [None] * 24)
            )
            for prefix in _SUB_PREFIXES
        }
        date_an = analogs[analogs["date"] == d]
        sub_strips["da_onpk"], _ = _he_strip_for_date(date_an)
        for key in ("outages", "m3"):
            if (
                feats[key] is not None
                and target_feats[key] is not None
                and stds[key] > 0
            ):
                z = (feats[key] - target_feats[key]) / stds[key]
                sub_strips[key] = _shade_z(z) * 24
            else:
                sub_strips[key] = "·" * 24
        for key, _, _, vw, dw, _, _ in _COLS:
            cell_w = vw + 1 + dw + 1 + SIGMA_W
            content = sub_strips.get(key, "")
            parts.append(f"{content:^{cell_w}}")
        print("  ".join(parts))

    print(_fmt_row("-", "TARGET", "-", "-", "-", target_feats, None, "-"))
    print(sep)
    n_displayed = min(n_show, len(rows_features))
    for i, r in enumerate(rows_features[:n_show]):
        d_str = pd.Timestamp(r["date"]).strftime("%a %b-%d %Y")
        date_an = analogs[analogs["date"] == r["date"]]
        _, n_hours = _he_strip_for_date(date_an)
        print(
            _fmt_row(
                str(r["rank"]),
                d_str,
                f"{r['mean_distance']:.4f}",
                f"{r['summed_weight']:.4f}",
                f"{r['w']:.3f}",
                r,
                target_feats,
                str(n_hours),
            )
        )
        _print_sub_strips(r["date"], r)
        if i < n_displayed - 1:
            print()
    print(sep)
    print(
        _fmt_row(
            "-",
            "Like-Day Avg (wtd)",
            "-",
            f"{total_w:.4f}",
            "1.000",
            avg,
            target_feats,
            "-",
        )
    )
    print(sep)

    n_dates = len(rows_features)
    shown_w = sum(r["w"] for r in rows_features[: min(n_show, n_dates)])
    print(
        f"\n  Showing top {min(n_show, n_dates)} of {n_dates} unique analog dates  "
        f"|  Top-{n_show} weight share: {shown_w:.1%}  "
        f"|  sigma-gap = (analog - target) / pool_std  "
        f"|  Like-Day Avg uses all {n_dates} dates"
    )


def print_analogs(analogs: pd.DataFrame, target_date: date, hub: str) -> None:
    """Top analog days table (simple aggregated view, kept for callers
    that don't need the daily-feature comparison."""
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
    print_header("DA LMP LIKE-DAY FORECAST — Western Hub ($/MWh)", "=", 120)

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
        for label, key in (
            ("80%PI", "coverage_80pct"),
            ("90%PI", "coverage_90pct"),
            ("98%PI", "coverage_98pct"),
        ):
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

    print()
    print_divider("=", 120, dim=False)
    print()


_SHAPE_DISPLAY: tuple[tuple[str, str | None, str | None, str], ...] = (
    ("block_mean_err", None, "block_mean_err", "+.2f"),
    ("block_mean_abs_err", None, "block_mean_abs_err", ".2f"),
    ("peak_height_err", "peak_height_err", "peak_height_err_onpeak", "+.2f"),
    (
        "peak_at_actual_hour_err",
        "peak_at_actual_hour_err",
        "peak_at_actual_hour_err_onpeak",
        "+.2f",
    ),
    ("time_of_peak_err (h)", "time_of_peak_err", "time_of_peak_err_onpeak", "+.0f"),
    ("valley_height_err", "valley_height_err", "valley_height_err_onpeak", "+.2f"),
    (
        "valley_at_actual_hour_err",
        "valley_at_actual_hour_err",
        "valley_at_actual_hour_err_onpeak",
        "+.2f",
    ),
    (
        "time_of_valley_err (h)",
        "time_of_valley_err",
        "time_of_valley_err_onpeak",
        "+.0f",
    ),
    ("peak_window_mae", "peak_window_mae", "peak_window_mae_onpeak", ".2f"),
    ("first_diff_mae", "first_diff_mae", "first_diff_mae_onpeak", ".2f"),
    ("variogram_score_p05", "variogram_score_p05", "variogram_score_p05_onpeak", ".4f"),
    ("peak_outside_onpeak", None, "peak_outside_onpeak", ".0f"),
    ("valley_outside_onpeak", None, "valley_outside_onpeak", ".0f"),
)


def print_shape(shape: dict | None, shape_onpeak: dict | None) -> None:
    """SHAPE METRICS block — full 24h vs HE8-23 on-peak side by side.

    Skips silently when both dicts are empty/None (no actuals available).
    """
    shape = shape or {}
    shape_onpeak = shape_onpeak or {}
    if not shape and not shape_onpeak:
        return

    print_header("SHAPE METRICS — Western Hub", "=", 120)
    label_w = 28
    col_w = 18
    head = (
        f"  {'metric':<{label_w}} {'full 24h':>{col_w}} {'HE8-23 (on-peak)':>{col_w}}"
    )
    print(head)
    print("-" * len(head))

    def _fmt(val: float | None, fmt: str) -> str:
        if val is None or (isinstance(val, float) and not np.isfinite(val)):
            return f"{'-':>{col_w}}"
        return f"{format(val, fmt):>{col_w}}"

    for label, full_key, onpeak_key, fmt in _SHAPE_DISPLAY:
        full_val = shape.get(full_key) if full_key else None
        onpeak_val = shape_onpeak.get(onpeak_key) if onpeak_key else None
        print(f"  {label:<{label_w}} {_fmt(full_val, fmt)} {_fmt(onpeak_val, fmt)}")

    print()
    print(
        "  variogram is shape-only — translation-invariant. peak_outside_onpeak=1"
        " means the daily peak fell outside HE8-23 (winter morning peaks)."
    )
    print()
    print_divider("=", 120, dim=False)
    print()


def print_quantiles(table: pd.DataFrame) -> None:
    """Quantile bands table."""
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
