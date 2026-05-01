"""View-model builders for the binding-constraint MCP endpoints.

Two endpoints, two builders:

  ``GET /views/constraints_da_network``        → ``build_da_network_view_model``
  ``GET /views/constraints_rt_dart_network``   → ``build_rt_dart_network_view_model``

Both consume rows from ``pjm_da_modelling_cleaned.pjm_constraints_hourly_pivot``
already enriched by ``data.constraint_network_match.match_constraints_to_branches``.
The DA view is forward-looking (one row per constraint for a target date);
the RT+DART view pivots RT and DART side-by-side per (date, constraint,
contingency) and ranks by ``|dart_total_price|``.
"""
from __future__ import annotations

from datetime import date
from typing import Optional

import numpy as np
import pandas as pd


_HE_COLS = [f"he{h:02d}" for h in range(1, 25)]


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _si(val) -> Optional[int]:
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else int(f)
    except (TypeError, ValueError):
        return None


def _sf(val) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _ss(val) -> Optional[str]:
    """Safe string — None for NaN / pd.NaT / empty / non-string-coercible."""
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    s = str(val).strip()
    return s if s and s.lower() != "nan" else None


def _row_dict(row: pd.Series, *, market_prefix: Optional[str] = None) -> dict:
    """Common fields shared between DA and RT/DART rows.

    ``market_prefix`` (e.g. ``"da"``, ``"rt"``, ``"dart"``) namespaces the
    price/hours columns so the pivoted RT+DART view can carry both sides.
    """
    p = market_prefix or ""
    pfx = f"{p}_" if p else ""
    return {
        f"{pfx}total_price": _sf(row.get("total_price")),
        f"{pfx}total_hours": _si(row.get("total_hours")),
        f"{pfx}onpeak_price": _sf(row.get("onpeak_price")),
        f"{pfx}onpeak_hours": _si(row.get("onpeak_hours")),
        f"{pfx}offpeak_price": _sf(row.get("offpeak_price")),
        f"{pfx}offpeak_hours": _si(row.get("offpeak_hours")),
    }


def _network_fields(row: pd.Series) -> dict:
    """Network-match metadata + parsed parser fields for a constraint row."""
    return {
        "parser_dialect": _ss(row.get("parser_dialect")),
        "parsed_equipment_type": _ss(row.get("parsed_equipment_type")),
        "parsed_voltage_kv": _si(row.get("parsed_voltage_kv")),
        "parsed_from_station": _ss(row.get("parsed_from_station")),
        "parsed_to_station": _ss(row.get("parsed_to_station")),
        "parsed_single_station": _ss(row.get("parsed_single_station")),
        "from_bus_psse": _si(row.get("from_bus_psse")),
        "to_bus_psse": _si(row.get("to_bus_psse")),
        "rating_mva": _sf(row.get("rating_mva")),
        "neighbor_count": _si(row.get("neighbor_count")),
        "match_status": _ss(row.get("network_match_status")),
    }


def _coverage(df: pd.DataFrame) -> dict:
    """Match coverage stats — counts and percentages."""
    total = len(df)
    if total == 0:
        return {"total": 0, "matched": 0, "ambiguous": 0, "unmatched": 0,
                "interface": 0, "match_rate_pct": 0.0}
    counts = df["network_match_status"].value_counts().to_dict()
    matched = int(counts.get("matched", 0))
    ambiguous = int(counts.get("ambiguous", 0))
    unmatched = int(counts.get("unmatched", 0))
    interface = int(counts.get("interface", 0))
    return {
        "total": total,
        "matched": matched,
        "ambiguous": ambiguous,
        "unmatched": unmatched,
        "interface": interface,
        # Network-class only (excludes interface from numerator and denominator)
        "match_rate_pct": round(
            100 * (matched + ambiguous) / max(total - interface, 1), 1
        ) if total > interface else 0.0,
    }


def _constraint_record(
    row: pd.Series,
    branches_df: pd.DataFrame,
    *,
    market_prefix: Optional[str] = None,
    max_neighbors: int = 10,
    include_he: bool = True,
    binding_hours: Optional[list[int]] = None,
) -> dict:
    """Compose one constraint row (network-enriched) for the response.

    Neighbors are 2-hop, ≥230 kV — k=1 misses parallel-path topology and k=3
    explodes; the HV filter keeps radial 138 kV taps out of the result.

    When ``binding_hours`` is set (Tier 3 funnel mode), the response is
    trimmed: ``hourly`` (24-list) is replaced with ``hourly_binding`` (dict
    keyed on the binding HE values), and verbose ``neighbors`` are dropped
    in favor of ``neighbor_bus_ids`` (a flat int list). When unset, the
    response shape is unchanged for backward compat with callers like the
    existing ``pjm-da-constraints-brief`` slash command.

    ``neighbor_bus_ids`` is always emitted (additive, safe).
    """
    # Late import — keeps this module importable without parquets present
    from backend.mcp_server.data.network_match import k_hop_neighbors

    rec = {
        "date": str(row["date"]) if pd.notna(row.get("date")) else None,
        "constraint_name": _ss(row.get("constraint_name")),
        "contingency": _ss(row.get("contingency")),
        "reported_name": _ss(row.get("reported_name")),
    }
    rec.update(_row_dict(row, market_prefix=market_prefix))
    rec.update(_network_fields(row))

    fb, tb = rec.get("from_bus_psse"), rec.get("to_bus_psse")
    neighbors: list[dict] = []
    if fb is not None and tb is not None and max_neighbors > 0:
        neighbors = k_hop_neighbors(
            fb, tb, branches_df, k=2, min_voltage_kv=230, max_n=max_neighbors,
        )

    # Always emit flat bus-id list (Tier 4 handoff; additive, safe).
    nb_ids: list[int] = []
    seen: set[int] = set()
    for nb in neighbors:
        for k in ("from_bus", "to_bus"):
            v = nb.get(k)
            if v is None:
                continue
            try:
                vi = int(v)
            except (TypeError, ValueError):
                continue
            if vi == fb or vi == tb or vi in seen:
                continue
            seen.add(vi)
            nb_ids.append(vi)
    rec["neighbor_bus_ids"] = nb_ids[:10]

    if binding_hours:
        # Funnel mode — trim verbose fields, surface only the binding HE prices.
        bh_dict = {}
        for h in binding_hours:
            v = _sf(row.get(f"he{h:02d}"))
            if v is not None:
                bh_dict[int(h)] = v
        rec["hourly_binding"] = bh_dict
        rec["binding_price"] = sum(bh_dict.values()) if bh_dict else 0.0
        rec["binding_hours_bound"] = sum(
            1 for v in bh_dict.values() if v is not None and abs(v) > 0
        )
        # Drop verbose neighbors (kept the bus-id list above)
        rec["neighbors"] = []
    else:
        # Default mode — preserve existing shape for backward compat.
        if include_he:
            rec["hourly"] = [_sf(row.get(c)) for c in _HE_COLS]
        rec["neighbors"] = neighbors
    return rec


# ─── Builder 1 — DA forward view ─────────────────────────────────────────────


def build_da_network_view_model(
    enriched_df: pd.DataFrame,
    branches_df: pd.DataFrame,
    target_date: date,
    *,
    top_n: int = 20,
    max_neighbors: int = 3,
    binding_hours: Optional[list[int]] = None,
) -> dict:
    """View model for ``GET /views/constraints_da_network``.

    Sections produced:
      - match_coverage         : counts by status
      - matched_constraints    : unique PSS/E branch matches (top_n by total_price)
      - ambiguous_constraints  : multi-candidate matches (first PSS/E shown)
      - unmatched_constraints  : facility didn't parse to a known PSS/E branch
      - interface_constraints  : zone/interface names (no branch match attempted)

    When ``binding_hours`` is provided (Tier 3 funnel mode), matched and
    ambiguous constraints are re-ranked by sum-over-binding-hours shadow
    price instead of total_price, and per-record HE list collapses to the
    binding hours only.
    """
    if enriched_df is None or enriched_df.empty:
        return {
            "target_date": str(target_date),
            "binding_hours": list(binding_hours) if binding_hours else None,
            "match_coverage": _coverage(pd.DataFrame()),
            "matched_constraints": [],
            "ambiguous_constraints": [],
            "unmatched_constraints": [],
            "interface_constraints": [],
        }

    df = enriched_df.copy()
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce")
    # PJM shadow prices are negative; "most binding" = largest |total_price|.
    # Sort by absolute value so -$1,730 comes before -$35 (was inverted).
    df["total_price_abs"] = df["total_price"].abs()

    # When in funnel mode, compute binding_price (sum over binding HEs) and
    # binding_price_abs (sort key — shadow prices in PJM are negative, so we
    # want the largest |binding_price| first to surface the most-bound
    # constraints during those hours).
    if binding_hours:
        he_cols = [f"he{h:02d}" for h in binding_hours]
        present_cols = [c for c in he_cols if c in df.columns]
        if present_cols:
            df["binding_price"] = (
                df[present_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
            )
        else:
            df["binding_price"] = 0.0
        df["binding_price_abs"] = df["binding_price"].abs()

    funnel_sort = "binding_price_abs" if binding_hours else "total_price_abs"

    sections = {}
    for status, key in [
        ("matched", "matched_constraints"),
        ("ambiguous", "ambiguous_constraints"),
        ("unmatched", "unmatched_constraints"),
        ("interface", "interface_constraints"),
    ]:
        if status in ("matched", "ambiguous"):
            sub = df[df["network_match_status"] == status].sort_values(
                funnel_sort, ascending=False, na_position="last",
            )
        else:
            # Unmatched / interface — by absolute price too (was inverted).
            sub = df[df["network_match_status"] == status].sort_values(
                "total_price_abs", ascending=False, na_position="last",
            )
        if status in ("matched", "ambiguous") and top_n:
            sub = sub.head(top_n)
        sections[key] = [
            _constraint_record(r, branches_df, market_prefix="da",
                               max_neighbors=max_neighbors,
                               binding_hours=binding_hours)
            for _, r in sub.iterrows()
        ]

    return {
        "target_date": str(target_date),
        "binding_hours": list(binding_hours) if binding_hours else None,
        "match_coverage": _coverage(df),
        **sections,
    }


# ─── Builder 2 — RT + DART backward view ─────────────────────────────────────


def _pivot_rt_dart(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot long-form RT+DART rows into one row per (date, constraint,
    contingency), with ``rt_*`` and ``dart_*`` columns side by side.

    Network-match metadata is identical across markets for a given constraint
    string, so we keep one copy from whichever side appears first.
    """
    if df.empty:
        return df

    keys = ["date", "constraint_name", "contingency"]
    common_cols = [
        "reported_name", "parser_dialect", "parsed_equipment_type",
        "parsed_voltage_kv", "parsed_from_station", "parsed_to_station",
        "parsed_single_station", "from_bus_psse", "to_bus_psse",
        "rating_mva", "neighbor_count", "network_match_status",
    ]
    metric_cols = [
        "total_price", "total_hours", "onpeak_price", "onpeak_hours",
        "offpeak_price", "offpeak_hours",
    ] + _HE_COLS

    rt = df[df["market"] == "RT"].set_index(keys)
    dart = df[df["market"] == "DART"].set_index(keys)

    # Outer join on the key set; take metrics from each side, common from RT
    # then fill from DART for keys missing on the RT side.
    merged_metrics = rt[metric_cols].add_prefix("rt_").join(
        dart[metric_cols].add_prefix("dart_"),
        how="outer",
    )

    common = (
        rt[common_cols]
        .combine_first(dart[common_cols])
    )

    out = merged_metrics.join(common, how="left").reset_index()
    return out


def build_rt_dart_network_view_model(
    enriched_df: pd.DataFrame,
    branches_df: pd.DataFrame,
    start_date: date,
    end_date: date,
    *,
    top_n: int = 30,
    max_neighbors: int = 3,
    morning_mode: bool = False,
) -> dict:
    """View model for ``GET /views/constraints_rt_dart_network``.

    Default mode — sections sorted by ``ABS(dart_total_price)`` desc:
      - matched_constraints, ambiguous_constraints, unmatched_constraints,
        interface_constraints (per-day rows, existing shape)

    Morning mode (``morning_mode=True``) — used by the pre-DA brief:
      - rows roll up across dates per (constraint_name, contingency)
      - only matched + ambiguous returned, in a single ``worst_binders``
        list sorted by ``ABS(rt_total_price_week)`` desc
      - each record carries ``binding_day_count``, ``binding_he_pattern``
        (24-int histogram + ranges label), ``daily_breakdown``, ``bus_ids``
        (seed + 1-hop ≥230 kV neighbors)
    """
    if enriched_df is None or enriched_df.empty:
        empty: dict = {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "morning_mode": morning_mode,
            "match_coverage": _coverage(pd.DataFrame()),
        }
        if morning_mode:
            empty["worst_binders"] = []
        else:
            empty.update({
                "matched_constraints": [],
                "ambiguous_constraints": [],
                "unmatched_constraints": [],
                "interface_constraints": [],
            })
        return empty

    pivoted = _pivot_rt_dart(enriched_df)

    pivoted["dart_abs"] = pd.to_numeric(
        pivoted.get("dart_total_price"), errors="coerce",
    ).abs().fillna(0)
    pivoted["rt_total_price"] = pd.to_numeric(
        pivoted.get("rt_total_price"), errors="coerce",
    )

    if morning_mode:
        # Roll up across dates per (constraint_name, contingency).
        net = pivoted[pivoted["network_match_status"].isin(["matched", "ambiguous"])]
        records: list[dict] = []
        if not net.empty:
            for _, group in net.groupby(
                ["constraint_name", "contingency"], dropna=False,
            ):
                records.append(
                    _morning_rollup_record(
                        group, branches_df, max_neighbors=max_neighbors,
                    )
                )
            records.sort(
                key=lambda r: abs(r.get("rt_total_price_week") or 0),
                reverse=True,
            )
            if top_n:
                records = records[:top_n]
        return {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "morning_mode": True,
            "lookback_days": (end_date - start_date).days + 1,
            "match_coverage": _coverage(pivoted),
            "worst_binders": records,
        }

    sections = {}
    for status, key in [
        ("matched", "matched_constraints"),
        ("ambiguous", "ambiguous_constraints"),
        ("unmatched", "unmatched_constraints"),
        ("interface", "interface_constraints"),
    ]:
        sub = pivoted[pivoted["network_match_status"] == status].sort_values(
            ["dart_abs", "rt_total_price"], ascending=[False, False],
        )
        if status in ("matched", "ambiguous") and top_n:
            sub = sub.head(top_n)
        sections[key] = [
            _rt_dart_record(r, branches_df, max_neighbors=max_neighbors)
            for _, r in sub.iterrows()
        ]

    return {
        "start_date": str(start_date),
        "end_date": str(end_date),
        "morning_mode": False,
        "match_coverage": _coverage(pivoted),
        **sections,
    }


def _format_he_range(hours: list[int]) -> str:
    """Compact label for a list of HEs.

    [14,15,16,17,18,19] -> 'HE 14-19'.
    [6,7,8,14,15,16]    -> 'HE 6-8, 14-16'.
    """
    if not hours:
        return "(none)"
    hours = sorted(set(int(h) for h in hours))
    ranges: list[tuple[int, int]] = []
    start = prev = hours[0]
    for h in hours[1:]:
        if h == prev + 1:
            prev = h
        else:
            ranges.append((start, prev))
            start = prev = h
    ranges.append((start, prev))
    return "HE " + ", ".join(f"{a}-{b}" if a != b else f"{a}" for a, b in ranges)


def _morning_rollup_record(
    group: pd.DataFrame,
    branches_df: pd.DataFrame,
    *,
    max_neighbors: int = 5,
) -> dict:
    """Aggregate one constraint's rows (across the lookback window) into a
    single morning-mode record. Group is rows for a single (constraint_name,
    contingency) across the dates in the window.
    """
    from backend.mcp_server.data.network_match import k_hop_neighbors

    # Per-HE counts and sums (RT-realized — morning brief is RT-driven)
    he_counts: dict[int, int] = {}
    he_sums: dict[int, float] = {}
    for h in range(1, 25):
        col = f"rt_he{h:02d}"
        if col not in group.columns:
            continue
        vals = pd.to_numeric(group[col], errors="coerce").fillna(0)
        he_counts[h] = int((vals.abs() > 0.01).sum())
        he_sums[h] = float(vals.abs().sum())

    binding_he_hours = sorted([h for h, c in he_counts.items() if c >= 1])
    histogram = [he_counts.get(h, 0) for h in range(1, 25)]

    # binding_day_count: distinct dates with rt_total_hours > 0
    rt_hours = pd.to_numeric(
        group.get("rt_total_hours", pd.Series(dtype=float)), errors="coerce",
    ).fillna(0)
    binding_day_count = int((rt_hours > 0).sum())

    # daily_breakdown sorted by date asc
    daily = []
    for _, row in group.sort_values("date").iterrows():
        d = row.get("date")
        if pd.notna(d):
            daily.append({
                "date": str(d),
                "rt_total_price": _sf(row.get("rt_total_price")),
                "rt_total_hours": _si(row.get("rt_total_hours")),
                "dart_total_price": _sf(row.get("dart_total_price")),
            })

    # week-aggregate scalars
    rt_prices = pd.to_numeric(
        group.get("rt_total_price", pd.Series(dtype=float)), errors="coerce",
    ).fillna(0)
    rt_total_week = float(rt_prices.sum())
    dart_prices = pd.to_numeric(
        group.get("dart_total_price", pd.Series(dtype=float)), errors="coerce",
    )
    dart_abs_week = float(dart_prices.abs().fillna(0).sum())

    # Representative row for facility/bus metadata (use most-recent date)
    rep = group.sort_values("date").iloc[-1]

    rec: dict = {
        "constraint_name": _ss(rep.get("constraint_name")),
        "contingency": _ss(rep.get("contingency")),
        "reported_name": _ss(rep.get("reported_name")),
        "rt_total_price_week": rt_total_week,
        "dart_total_price_week_abs": dart_abs_week,
        "binding_day_count": binding_day_count,
        "binding_he_pattern": {
            "hours": binding_he_hours,
            "histogram": histogram,
            "label": _format_he_range(binding_he_hours),
        },
        "daily_breakdown": daily,
    }
    rec.update(_network_fields(rep))

    # bus_ids: seed + 1-hop ≥230 kV neighbors, deduped
    fb, tb = rec.get("from_bus_psse"), rec.get("to_bus_psse")
    bus_ids: list[int] = []
    seen: set[int] = set()
    for v in (fb, tb):
        if v is not None and v not in seen:
            seen.add(v)
            bus_ids.append(v)
    if fb is not None and tb is not None and max_neighbors > 0:
        neighbors = k_hop_neighbors(
            fb, tb, branches_df, k=1, min_voltage_kv=230, max_n=max_neighbors,
        )
        for nb in neighbors:
            for k in ("from_bus", "to_bus"):
                v = nb.get(k)
                if v is None:
                    continue
                try:
                    vi = int(v)
                except (TypeError, ValueError):
                    continue
                if vi in seen:
                    continue
                seen.add(vi)
                bus_ids.append(vi)
    rec["bus_ids"] = bus_ids
    return rec


def _rt_dart_record(
    row: pd.Series, branches_df: pd.DataFrame, *, max_neighbors: int,
) -> dict:
    """Wide row format for the RT+DART pivoted view (2-hop ≥230 kV neighbors)."""
    from backend.mcp_server.data.network_match import k_hop_neighbors

    rec = {
        "date": str(row["date"]) if pd.notna(row.get("date")) else None,
        "constraint_name": _ss(row.get("constraint_name")),
        "contingency": _ss(row.get("contingency")),
        "reported_name": _ss(row.get("reported_name")),
        "rt_total_price": _sf(row.get("rt_total_price")),
        "rt_total_hours": _si(row.get("rt_total_hours")),
        "rt_onpeak_price": _sf(row.get("rt_onpeak_price")),
        "rt_offpeak_price": _sf(row.get("rt_offpeak_price")),
        "dart_total_price": _sf(row.get("dart_total_price")),
        "dart_total_hours": _si(row.get("dart_total_hours")),
        "dart_onpeak_price": _sf(row.get("dart_onpeak_price")),
        "dart_offpeak_price": _sf(row.get("dart_offpeak_price")),
    }
    rec.update(_network_fields(row))

    fb, tb = rec.get("from_bus_psse"), rec.get("to_bus_psse")
    if fb is not None and tb is not None and max_neighbors > 0:
        rec["neighbors"] = k_hop_neighbors(
            fb, tb, branches_df, k=2, min_voltage_kv=230, max_n=max_neighbors,
        )
    else:
        rec["neighbors"] = []
    return rec
