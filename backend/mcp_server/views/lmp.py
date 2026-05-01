"""View-model builders for the DA-LMP MCP endpoints.

Two endpoints, two builders:

  ``GET /views/lmp_da_hub_summary``    → ``build_lmp_da_hub_summary_view_model``
  ``GET /views/lmp_da_outage_overlap`` → ``build_lmp_da_outage_overlap_view_model``

The hub summary aggregates ``pjm_lmps_hourly`` (market='da') into one row
per hub for a target date, decomposing total / energy / congestion / loss
across onpeak / offpeak / peak-hour. The outage-overlap view walks the
top binding constraints, expands each to its 2-hop ≥230 kV neighbor set
on the PSS/E network, and reports any active or upcoming transmission
outages (target_date ± 7d) on those branches.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd


# PJM standard 5x16 onpeak: HE 8 through HE 23. We apply it daily regardless
# of weekday — the brief is a one-day operator view, not a billing aggregate.
_ONPEAK_HE = set(range(8, 24))  # 8..23 inclusive


def _sf(val) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _si(val) -> Optional[int]:
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else int(f)
    except (TypeError, ValueError):
        return None


# ─── Hub summary ─────────────────────────────────────────────────────────────


def _hub_record(group: pd.DataFrame) -> dict:
    """Aggregate one hub's hourly LMPs to onpeak / offpeak / flat / peak-hour."""
    hub = group["hub"].iloc[0]
    onpeak = group[group["hour_ending"].isin(_ONPEAK_HE)]
    offpeak = group[~group["hour_ending"].isin(_ONPEAK_HE)]

    def _avg(df: pd.DataFrame, col: str) -> Optional[float]:
        if df.empty:
            return None
        return _sf(df[col].mean())

    # Peak hour = max |lmp_total| within the day
    if group.empty:
        peak = None
    else:
        peak_idx = group["lmp_total"].abs().idxmax()
        peak = group.loc[peak_idx]

    onpeak_total = _avg(onpeak, "lmp_total")
    onpeak_cong = _avg(onpeak, "lmp_congestion_price")
    cong_pct = (
        abs(onpeak_cong) / abs(onpeak_total)
        if onpeak_total and abs(onpeak_total) > 1e-6 and onpeak_cong is not None
        else None
    )

    return {
        "hub": hub,
        "onpeak_total": onpeak_total,
        "onpeak_energy": _avg(onpeak, "lmp_system_energy_price"),
        "onpeak_congestion": onpeak_cong,
        "onpeak_loss": _avg(onpeak, "lmp_marginal_loss_price"),
        "offpeak_total": _avg(offpeak, "lmp_total"),
        "offpeak_energy": _avg(offpeak, "lmp_system_energy_price"),
        "offpeak_congestion": _avg(offpeak, "lmp_congestion_price"),
        "offpeak_loss": _avg(offpeak, "lmp_marginal_loss_price"),
        "flat_total": _avg(group, "lmp_total"),
        "flat_energy": _avg(group, "lmp_system_energy_price"),
        "flat_congestion": _avg(group, "lmp_congestion_price"),
        "peak_hour": _si(peak["hour_ending"]) if peak is not None else None,
        "peak_total": _sf(peak["lmp_total"]) if peak is not None else None,
        "peak_congestion": _sf(peak["lmp_congestion_price"]) if peak is not None else None,
        "congestion_pct_of_total": round(cong_pct, 3) if cong_pct is not None else None,
    }


def build_lmp_da_hub_summary_view_model(
    df: pd.DataFrame,
    target_date: date,
    *,
    high_congestion_threshold: float = 0.10,
) -> dict:
    """View model for ``GET /views/lmp_da_hub_summary``.

    Sections:
      - target_date
      - hub_count, hour_count
      - market_avg_onpeak / market_avg_offpeak : flat averages across hubs
      - high_congestion_count : hubs where |onpeak_congestion| / |onpeak_total|
                                > ``high_congestion_threshold`` (default 10%)
      - hubs : one record per hub, sorted by |onpeak_congestion| desc

    Hubs with all-NULL DA LMP are dropped (DART rows for hubs missing RT
    counterparts can produce empty buckets — this is DA-only so it's rare).
    """
    if df is None or df.empty:
        return {
            "target_date": str(target_date),
            "hub_count": 0,
            "hour_count": 0,
            "market_avg_onpeak": None,
            "market_avg_offpeak": None,
            "high_congestion_count": 0,
            "hubs": [],
            "error": "No DA LMP data for target_date.",
        }

    df = df.copy()
    df["lmp_total"] = pd.to_numeric(df["lmp_total"], errors="coerce")
    df["lmp_system_energy_price"] = pd.to_numeric(df["lmp_system_energy_price"], errors="coerce")
    df["lmp_congestion_price"] = pd.to_numeric(df["lmp_congestion_price"], errors="coerce")
    df["lmp_marginal_loss_price"] = pd.to_numeric(df["lmp_marginal_loss_price"], errors="coerce")
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")

    hubs = [_hub_record(g) for _, g in df.groupby("hub", sort=False)]
    # Drop hubs with no usable rows
    hubs = [h for h in hubs if h["flat_total"] is not None]

    hubs.sort(
        key=lambda h: abs(h["onpeak_congestion"] or 0),
        reverse=True,
    )

    onpeak = df[df["hour_ending"].isin(_ONPEAK_HE)]
    offpeak = df[~df["hour_ending"].isin(_ONPEAK_HE)]
    market_onpeak = {
        "total": _sf(onpeak["lmp_total"].mean()) if not onpeak.empty else None,
        "energy": _sf(onpeak["lmp_system_energy_price"].mean()) if not onpeak.empty else None,
        "congestion": _sf(onpeak["lmp_congestion_price"].mean()) if not onpeak.empty else None,
        "loss": _sf(onpeak["lmp_marginal_loss_price"].mean()) if not onpeak.empty else None,
    }
    market_offpeak = {
        "total": _sf(offpeak["lmp_total"].mean()) if not offpeak.empty else None,
        "energy": _sf(offpeak["lmp_system_energy_price"].mean()) if not offpeak.empty else None,
        "congestion": _sf(offpeak["lmp_congestion_price"].mean()) if not offpeak.empty else None,
        "loss": _sf(offpeak["lmp_marginal_loss_price"].mean()) if not offpeak.empty else None,
    }

    high_cong = sum(
        1 for h in hubs
        if h["congestion_pct_of_total"] is not None
        and h["congestion_pct_of_total"] > high_congestion_threshold
    )

    return {
        "target_date": str(target_date),
        "hub_count": len(hubs),
        "hour_count": int(df["hour_ending"].nunique()),
        "market_avg_onpeak": market_onpeak,
        "market_avg_offpeak": market_offpeak,
        "high_congestion_threshold": high_congestion_threshold,
        "high_congestion_count": high_cong,
        "hubs": hubs,
    }


# ─── Tier 1 — daily summary (hub-grain, with drilldown handoff) ──────────────


def _delta(cur: Optional[float], prior: Optional[float]) -> Optional[float]:
    """Return cur - prior, or None if either side is missing."""
    if cur is None or prior is None:
        return None
    return float(cur) - float(prior)


def build_lmps_daily_summary_view_model(
    df: pd.DataFrame,
    target_date: date,
    *,
    prior_period_df: pd.DataFrame | None = None,
    prior_period_date: date | None = None,
    top_n_drilldown: int = 5,
    high_congestion_threshold: float = 0.10,
) -> dict:
    """View model for ``GET /views/lmps_daily_summary`` (Tier 1).

    Wraps ``build_lmp_da_hub_summary_view_model`` and adds the funnel
    handoff field ``top_zones_for_drilldown`` — the top ``top_n_drilldown``
    hubs by ``|onpeak_congestion|``, which Tier 2 reads as its hub filter.

    When ``prior_period_df`` is supplied (typically same-weekday-prior-week),
    each hub record gets a ``vs_peer`` block with onpeak/offpeak/congestion
    deltas, and a ``vs_peer_market`` block at top level for the RTO average.
    """
    vm = build_lmp_da_hub_summary_view_model(
        df, target_date, high_congestion_threshold=high_congestion_threshold,
    )
    hubs = vm.get("hubs") or []
    vm["top_zones_for_drilldown"] = [h["hub"] for h in hubs[:top_n_drilldown]]

    if prior_period_df is not None and not prior_period_df.empty:
        peer_date = prior_period_date or (target_date - timedelta(days=7))
        prior_vm = build_lmp_da_hub_summary_view_model(
            prior_period_df, peer_date,
            high_congestion_threshold=high_congestion_threshold,
        )
        prior_by_hub = {h["hub"]: h for h in (prior_vm.get("hubs") or [])}

        for h in hubs:
            prior = prior_by_hub.get(h["hub"])
            if not prior:
                continue
            h["vs_peer"] = {
                "peer_date": str(peer_date),
                "onpeak_total_delta": _delta(h.get("onpeak_total"), prior.get("onpeak_total")),
                "onpeak_congestion_delta": _delta(h.get("onpeak_congestion"), prior.get("onpeak_congestion")),
                "offpeak_total_delta": _delta(h.get("offpeak_total"), prior.get("offpeak_total")),
                "flat_total_delta": _delta(h.get("flat_total"), prior.get("flat_total")),
            }

        cur_mkt = vm.get("market_avg_onpeak") or {}
        prior_mkt = prior_vm.get("market_avg_onpeak") or {}
        cur_off = vm.get("market_avg_offpeak") or {}
        prior_off = prior_vm.get("market_avg_offpeak") or {}
        vm["vs_peer_market"] = {
            "peer_date": str(peer_date),
            "onpeak_total_delta": _delta(cur_mkt.get("total"), prior_mkt.get("total")),
            "onpeak_congestion_delta": _delta(cur_mkt.get("congestion"), prior_mkt.get("congestion")),
            "offpeak_total_delta": _delta(cur_off.get("total"), prior_off.get("total")),
        }

    return vm


# ─── Tier 2 — hourly drilldown (heatmap on the 5 hubs from Tier 1) ───────────


def build_lmps_hourly_summary_view_model(
    df: pd.DataFrame,
    target_date: date,
    *,
    hubs_filter: list[str] | None = None,
    binding_threshold: float = 25.0,
    max_binding_hours: int = 5,
    fallback_top_n: int = 3,
) -> dict:
    """View model for ``GET /views/lmps_hourly_summary`` (Tier 2).

    Sections produced:
      - hubs / hub_count / hour_count / binding_threshold_usd
      - peak_hour_callout : top 1-2 hours by max ``|congestion|`` across hubs
      - binding_hours_for_drilldown : 3-5 HE values where any hub crosses
        ``binding_threshold``; falls back to top ``fallback_top_n`` hours
        by max ``|congestion|`` if fewer than 3 hours qualify
      - hub_hour_grid : long-form (hub × hour_ending) — bounded by hub
        filter from Tier 1
      - per_hub_summary : one row per hub with max-cong / mean / count
    """
    if df is None or df.empty:
        return {
            "target_date": str(target_date),
            "hubs": list(hubs_filter or []),
            "hub_count": 0,
            "hour_count": 0,
            "binding_threshold_usd": binding_threshold,
            "peak_hour_callout": [],
            "binding_hours_for_drilldown": [],
            "hub_hour_grid": [],
            "per_hub_summary": [],
            "error": "No DA LMP data for target_date.",
        }

    df = df.copy()
    df["lmp_total"] = pd.to_numeric(df["lmp_total"], errors="coerce")
    df["lmp_system_energy_price"] = pd.to_numeric(
        df["lmp_system_energy_price"], errors="coerce")
    df["lmp_congestion_price"] = pd.to_numeric(
        df["lmp_congestion_price"], errors="coerce")
    df["lmp_marginal_loss_price"] = pd.to_numeric(
        df["lmp_marginal_loss_price"], errors="coerce")
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")
    df["abs_cong"] = df["lmp_congestion_price"].abs()

    hubs = list(df["hub"].dropna().unique()) if hubs_filter is None else list(hubs_filter)

    # Per-hour aggregates across the filtered hubs
    hourly_max = df.groupby("hour_ending", dropna=True)["abs_cong"].max()
    hourly_mean = df.groupby("hour_ending", dropna=True)["abs_cong"].mean()
    hourly_count_gt = df.groupby("hour_ending", dropna=True).apply(
        lambda g: int((g["abs_cong"] > binding_threshold).sum())
    )

    # peak_hour_callout — top 1-2 hours by max abs cong
    callout_rows = []
    if not hourly_max.empty:
        top_hours = hourly_max.sort_values(ascending=False).head(2)
        for he, max_cong in top_hours.items():
            row_at_max = df[(df["hour_ending"] == he)
                            & (df["abs_cong"] == max_cong)].iloc[0]
            callout_rows.append({
                "hour_ending": int(he),
                "max_abs_congestion": _sf(max_cong),
                "hub": row_at_max["hub"],
                "mean_abs_congestion_across_hubs": _sf(hourly_mean.get(he)),
                "hubs_with_congestion_gt_threshold": int(hourly_count_gt.get(he, 0)),
            })

    # binding_hours_for_drilldown — hours where any hub crosses threshold,
    # capped at max_binding_hours; fallback to top-N by max abs cong
    over_threshold_hours = sorted(
        int(he) for he, n in hourly_count_gt.items() if n > 0
    )
    if len(over_threshold_hours) >= 3:
        binding = over_threshold_hours[:max_binding_hours]
    elif not hourly_max.empty:
        binding = sorted(
            int(he) for he in hourly_max.sort_values(ascending=False)
            .head(fallback_top_n).index
        )
    else:
        binding = []

    # hub_hour_grid — long-form
    grid = []
    for _, r in df.sort_values(["hub", "hour_ending"]).iterrows():
        grid.append({
            "hub": r["hub"],
            "hour_ending": _si(r["hour_ending"]),
            "lmp_total": _sf(r["lmp_total"]),
            "lmp_system_energy_price": _sf(r["lmp_system_energy_price"]),
            "lmp_congestion_price": _sf(r["lmp_congestion_price"]),
            "lmp_marginal_loss_price": _sf(r["lmp_marginal_loss_price"]),
        })

    # per_hub_summary
    per_hub = []
    for hub_name, g in df.groupby("hub", sort=False):
        if g.empty:
            continue
        max_idx = g["abs_cong"].idxmax()
        per_hub.append({
            "hub": hub_name,
            "max_abs_congestion": _sf(g.loc[max_idx, "abs_cong"]),
            "max_abs_hour": _si(g.loc[max_idx, "hour_ending"]),
            "mean_congestion": _sf(g["lmp_congestion_price"].mean()),
            "binding_hours_count": int(
                (g["abs_cong"] > binding_threshold).sum()
            ),
        })
    per_hub.sort(key=lambda h: -(h["max_abs_congestion"] or 0))

    return {
        "target_date": str(target_date),
        "hubs": hubs,
        "hub_count": len(hubs),
        "hour_count": int(df["hour_ending"].nunique()),
        "binding_threshold_usd": binding_threshold,
        "peak_hour_callout": callout_rows,
        "binding_hours_for_drilldown": binding,
        "hub_hour_grid": grid,
        "per_hub_summary": per_hub,
    }


# ─── Pre-DA morning brief — Tier 1: 7-day DA→RT realization ──────────────────


def build_lmps_dart_realization_view_model(
    df: pd.DataFrame,
    end_date: date,
    *,
    lookback_days: int = 7,
    top_n_drilldown: int = 5,
    dart_threshold: float = 10.0,
) -> dict:
    """View model for ``GET /views/lmps_dart_realization`` (pre-DA brief Tier 1).

    Long-form ``df`` has rows per (date, hour_ending, hub, market) where market
    in {'da','rt','dart'}. Emits:

      - ``daily_summary``: per (date, hub) aggregates of total/cong for each market
      - ``hub_rollup``: per-hub aggregates over the window
      - ``worst_realized_hubs``: top-N hubs by ``sum_abs_dart_cong``, with
        ``peak_hours_of_day`` for HE-band scoping by Tier 2
      - ``top_zones_for_drilldown``: hub-name list (handoff for downstream)
      - ``window_aggregates``: market-wide rollups
    """
    start_date = end_date - timedelta(days=lookback_days - 1)

    if df is None or df.empty:
        return {
            "target_date": str(end_date),
            "start_date": str(start_date),
            "end_date": str(end_date),
            "lookback_days": lookback_days,
            "dart_threshold": dart_threshold,
            "daily_summary": [],
            "hub_rollup": [],
            "worst_realized_hubs": [],
            "top_zones_for_drilldown": [],
            "window_aggregates": {},
            "error": "No LMP data for window.",
        }

    df = df.copy()
    for col in [
        "lmp_total", "lmp_system_energy_price",
        "lmp_congestion_price", "lmp_marginal_loss_price",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")

    da = df[df["market"] == "da"]
    rt = df[df["market"] == "rt"]
    dart = df[df["market"] == "dart"].copy()

    # Per (date, hub) per market — daily mean
    keys = ["date", "hub"]
    da_d = da.groupby(keys, sort=False).agg(
        da_total=("lmp_total", "mean"),
        da_cong=("lmp_congestion_price", "mean"),
    )
    rt_d = rt.groupby(keys, sort=False).agg(
        rt_total=("lmp_total", "mean"),
        rt_cong=("lmp_congestion_price", "mean"),
    )
    dart_d = dart.groupby(keys, sort=False).agg(
        dart_total=("lmp_total", "mean"),
        dart_cong=("lmp_congestion_price", "mean"),
    )

    # Peak HE per (date, hub) by max |dart_cong|
    if not dart.empty:
        dart["abs_cong"] = dart["lmp_congestion_price"].abs()
        peak_he_idx = (
            dart.dropna(subset=["abs_cong"])
                .sort_values("abs_cong", ascending=False)
                .groupby(keys, sort=False)
                .head(1)[keys + ["hour_ending"]]
                .set_index(keys)
        )
        peak_he_idx = peak_he_idx.rename(columns={"hour_ending": "peak_he"})
    else:
        peak_he_idx = pd.DataFrame(columns=["peak_he"])

    daily = pd.concat([da_d, rt_d, dart_d, peak_he_idx], axis=1).reset_index()
    daily_records = []
    for _, row in daily.sort_values(keys).iterrows():
        daily_records.append({
            "date": str(row["date"]),
            "hub": row["hub"],
            "da_total": _sf(row.get("da_total")),
            "rt_total": _sf(row.get("rt_total")),
            "dart_total": _sf(row.get("dart_total")),
            "da_cong": _sf(row.get("da_cong")),
            "rt_cong": _sf(row.get("rt_cong")),
            "dart_cong": _sf(row.get("dart_cong")),
            "peak_he": _si(row.get("peak_he")),
        })

    # Per-hub rollup over the whole window
    rollups: list[dict] = []
    if not dart.empty:
        for hub_name, g in dart.groupby("hub", sort=False):
            avg_dart_cong = _sf(g["lmp_congestion_price"].mean())
            max_abs = _sf(g["abs_cong"].max())
            max_idx = g["abs_cong"].idxmax() if g["abs_cong"].notna().any() else None
            max_date = str(g.loc[max_idx, "date"]) if max_idx is not None else None
            hours_over = int((g["abs_cong"] > dart_threshold).sum())
            sum_abs = _sf(g["abs_cong"].sum())

            # 24-vec mean |dart_cong| per HE
            by_hod = g.groupby("hour_ending")["abs_cong"].mean()
            dart_by_hod = [
                _sf(by_hod.get(h)) if h in by_hod.index else 0.0
                for h in range(1, 25)
            ]

            # Trend slope: linear regression of daily |dart_cong| over day index
            daily_abs = g.groupby("date")["abs_cong"].mean().sort_index()
            if len(daily_abs) >= 3:
                x = np.arange(len(daily_abs), dtype=float)
                y = daily_abs.values.astype(float)
                slope = float(np.polyfit(x, y, 1)[0])
            else:
                slope = 0.0
            if slope > 0.5:
                trend = "widening"
            elif slope < -0.5:
                trend = "narrowing"
            else:
                trend = "stable"

            rollups.append({
                "hub": hub_name,
                "avg_dart_cong": avg_dart_cong,
                "max_abs_dart_cong": max_abs,
                "max_dart_date": max_date,
                "hours_over_threshold": hours_over,
                "sum_abs_dart_cong": sum_abs,
                "dart_cong_by_hod": dart_by_hod,
                "trend_signal": trend,
                "trend_slope": round(slope, 3),
            })

    rollups.sort(key=lambda r: -(r.get("sum_abs_dart_cong") or 0))

    # Worst-realized hubs handoff
    worst_records = []
    for r in rollups[:top_n_drilldown]:
        hod = r.get("dart_cong_by_hod") or [0.0] * 24
        ranked = sorted(range(24), key=lambda i: -(hod[i] or 0))[:3]
        peak_hods = sorted([i + 1 for i in ranked])
        worst_records.append({
            "hub": r["hub"],
            "sum_abs_dart_cong": r["sum_abs_dart_cong"],
            "peak_hours_of_day": peak_hods,
            "trend_signal": r["trend_signal"],
        })

    # Window aggregates
    if not dart.empty:
        avg_dart_all = _sf(dart["lmp_congestion_price"].mean())
        hub_days_over = int(
            (dart.groupby(["date", "hub"])["abs_cong"].max() > dart_threshold).sum()
        )
    else:
        avg_dart_all = None
        hub_days_over = 0

    window_agg = {
        "avg_dart_cong_all_hubs": avg_dart_all,
        "total_hub_days_over_threshold": hub_days_over,
        "hubs_with_widening_trend": sum(1 for r in rollups if r["trend_signal"] == "widening"),
        "hubs_with_narrowing_trend": sum(1 for r in rollups if r["trend_signal"] == "narrowing"),
        "hub_count": len(rollups),
        "day_count": int(dart["date"].nunique()) if not dart.empty else 0,
    }

    return {
        "target_date": str(end_date),
        "start_date": str(start_date),
        "end_date": str(end_date),
        "lookback_days": lookback_days,
        "dart_threshold": dart_threshold,
        "window_aggregates": window_agg,
        "daily_summary": daily_records,
        "hub_rollup": rollups,
        "worst_realized_hubs": worst_records,
        "top_zones_for_drilldown": [r["hub"] for r in worst_records],
    }


# ─── Outage overlap ──────────────────────────────────────────────────────────


def _outage_bucket(start: pd.Timestamp, end: pd.Timestamp, ref: pd.Timestamp) -> str:
    """Bucket an outage relative to the target date (window = ref ± 7d).

    'active'        : start ≤ ref ≤ end
    'starting_soon' : ref < start ≤ ref + 7d
    'ending_soon'   : ref ≤ end ≤ ref + 7d (start before ref)
    'other'         : in window but doesn't fit (shouldn't happen w/ our filter)
    """
    horizon = ref + pd.Timedelta(days=7)
    if pd.notna(start) and pd.notna(end) and start <= ref <= end:
        return "active"
    if pd.notna(start) and ref < start <= horizon:
        return "starting_soon"
    if pd.notna(end) and ref <= end <= horizon and (pd.isna(start) or start < ref):
        return "ending_soon"
    return "other"


def _outage_record(row: pd.Series, ref: pd.Timestamp) -> dict:
    start = pd.to_datetime(row.get("start_datetime"), errors="coerce")
    end = pd.to_datetime(row.get("end_datetime"), errors="coerce")
    bucket = _outage_bucket(start, end, ref)

    days_to_start = (start.date() - ref.date()).days if pd.notna(start) else None
    days_to_return = (end.date() - ref.date()).days if pd.notna(end) else None

    return {
        "ticket_id": _si(row.get("ticket_id")),
        "facility": row.get("facility_name", ""),
        "equip": row.get("equipment_type"),
        "kv": _si(row.get("voltage_kv")),
        "from_bus_psse": _si(row.get("from_bus_psse")),
        "to_bus_psse": _si(row.get("to_bus_psse")),
        "outage_state": row.get("outage_state"),
        "started": str(start.date()) if pd.notna(start) else None,
        "est_return": str(end.date()) if pd.notna(end) else None,
        "days_to_start": days_to_start,
        "days_to_return": days_to_return,
        "risk_flag": (
            str(row.get("risk", "")).strip().lower() == "yes"
            if row.get("risk") is not None else False
        ),
        "cause": (row.get("cause") or "").split(";")[0].strip() if row.get("cause") else "",
        "bucket": bucket,
    }


def build_lmp_da_outage_overlap_view_model(
    enriched_constraints_df: pd.DataFrame,
    enriched_outages_df: pd.DataFrame,
    branches_df: pd.DataFrame,
    target_date: date,
    *,
    top_n: int = 20,
    max_neighbors: int = 10,
) -> dict:
    """View model for ``GET /views/lmp_da_outage_overlap``.

    Walks the top-N binding DA constraints (matched + ambiguous), expands
    each to its 2-hop ≥230 kV neighbor set on the PSS/E network, then
    looks for transmission outages on the seed branch or any neighbor in
    the window ``[target_date, target_date + 7d]``.

    For each constraint, output:
      - constraint_name, contingency, total_price, hours bound
      - seed branch (from/to bus, MVA)
      - neighbors : up to ``max_neighbors`` 2-hop ≥230 kV branches
      - outages   : bucketed active / starting_soon / ending_soon (within
                    target_date ± 7d), each tagged with the seed/neighbor
                    branch they sit on

    Top-level summary counts how many of the top constraints have at least
    one overlapping outage — that's the "structural vs noise" signal.
    """
    from backend.mcp_server.data.network_match import (
        find_outages_on_branches,
        k_hop_neighbors,
    )

    if enriched_constraints_df is None or enriched_constraints_df.empty:
        return {
            "target_date": str(target_date),
            "top_n": top_n,
            "constraint_count": 0,
            "with_overlap_count": 0,
            "constraints": [],
            "error": "No DA constraint data for target_date.",
        }

    df = enriched_constraints_df.copy()
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce")

    # Restrict to constraints we can actually project onto the network
    df = df[df["network_match_status"].isin(["matched", "ambiguous"])]
    df = df.sort_values("total_price", ascending=False, na_position="last").head(top_n)

    if df.empty:
        return {
            "target_date": str(target_date),
            "top_n": top_n,
            "constraint_count": 0,
            "with_overlap_count": 0,
            "constraints": [],
        }

    ref_ts = pd.Timestamp(target_date)

    # Pre-filter outages to those overlapping [ref-now, ref+7d] window. Outage
    # frame may include only Active/Approved tickets — the caller decides.
    outages = enriched_outages_df.copy() if enriched_outages_df is not None else pd.DataFrame()
    if not outages.empty:
        outages["start_datetime"] = pd.to_datetime(outages.get("start_datetime"), errors="coerce")
        outages["end_datetime"] = pd.to_datetime(outages.get("end_datetime"), errors="coerce")
        horizon = ref_ts + pd.Timedelta(days=7)
        # Keep rows where [start, end] intersects [ref, ref+7d] OR is currently active
        mask = (
            ((outages["start_datetime"] <= horizon) & (outages["end_datetime"] >= ref_ts))
            | ((outages["start_datetime"].isna()) & (outages["end_datetime"] >= ref_ts))
            | ((outages["end_datetime"].isna()) & (outages["start_datetime"] <= horizon))
        )
        outages = outages[mask]

    constraints_out = []
    with_overlap = 0

    for _, row in df.iterrows():
        fb = _si(row.get("from_bus_psse"))
        tb = _si(row.get("to_bus_psse"))

        neighbors = (
            k_hop_neighbors(fb, tb, branches_df, k=2, min_voltage_kv=230, max_n=max_neighbors)
            if fb is not None and tb is not None
            else []
        )

        # Build the branch-key set: seed + neighbors
        branch_keys: list[tuple[int, int]] = []
        if fb is not None and tb is not None:
            branch_keys.append((fb, tb))
        for nb in neighbors:
            branch_keys.append((int(nb["from_bus"]), int(nb["to_bus"])))

        seed_key = frozenset((fb, tb)) if fb is not None and tb is not None else None
        nb_lookup: dict[frozenset, dict] = {}
        for nb in neighbors:
            key = frozenset((int(nb["from_bus"]), int(nb["to_bus"])))
            nb_lookup[key] = nb

        overlap_df = (
            find_outages_on_branches(outages, branch_keys)
            if not outages.empty else pd.DataFrame()
        )

        outage_recs: list[dict] = []
        for _, o in overlap_df.iterrows():
            o_fb = _si(o.get("from_bus_psse"))
            o_tb = _si(o.get("to_bus_psse"))
            o_key = frozenset((o_fb, o_tb)) if o_fb is not None and o_tb is not None else None
            rec = _outage_record(o, ref_ts)
            if o_key == seed_key:
                rec["on_branch"] = "seed"
                rec["branch_label"] = (
                    f"{row.get('parsed_from_station') or row.get('parsed_single_station') or '?'}"
                    f"→{row.get('parsed_to_station') or '?'}"
                    if row.get("parsed_from_station") or row.get("parsed_to_station")
                    else (row.get("parsed_single_station") or "seed")
                )
            elif o_key in nb_lookup:
                nb = nb_lookup[o_key]
                rec["on_branch"] = "neighbor"
                if nb.get("equipment_type") == "LINE":
                    rec["branch_label"] = f"{nb['from_name']}→{nb['to_name']}"
                else:
                    rec["branch_label"] = f"XFMR@{nb['from_name']}"
            else:
                rec["on_branch"] = "?"
                rec["branch_label"] = "?"
            outage_recs.append(rec)

        # Sort overlap by bucket (active first), then by kv desc
        bucket_rank = {"active": 0, "starting_soon": 1, "ending_soon": 2, "other": 3}
        outage_recs.sort(key=lambda r: (bucket_rank.get(r["bucket"], 9), -(r["kv"] or 0)))

        if outage_recs:
            with_overlap += 1

        constraints_out.append({
            "constraint_name": row.get("constraint_name"),
            "contingency": row.get("contingency"),
            "total_price": _sf(row.get("total_price")),
            "total_hours": _si(row.get("total_hours")),
            "parsed_voltage_kv": _si(row.get("parsed_voltage_kv")),
            "parsed_from_station": row.get("parsed_from_station"),
            "parsed_to_station": row.get("parsed_to_station"),
            "parsed_single_station": row.get("parsed_single_station"),
            "from_bus_psse": fb,
            "to_bus_psse": tb,
            "rating_mva": _sf(row.get("rating_mva")),
            "match_status": row.get("network_match_status"),
            "neighbor_count_k2_hv": len(neighbors),
            "neighbors": neighbors,
            "outage_overlap": outage_recs,
            "active_count": sum(1 for r in outage_recs if r["bucket"] == "active"),
            "starting_soon_count": sum(1 for r in outage_recs if r["bucket"] == "starting_soon"),
            "ending_soon_count": sum(1 for r in outage_recs if r["bucket"] == "ending_soon"),
        })

    return {
        "target_date": str(target_date),
        "window_days": 7,
        "top_n": top_n,
        "constraint_count": len(constraints_out),
        "with_overlap_count": with_overlap,
        "constraints": constraints_out,
    }
