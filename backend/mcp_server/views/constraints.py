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
        "parser_dialect": row.get("parser_dialect"),
        "parsed_equipment_type": row.get("parsed_equipment_type"),
        "parsed_voltage_kv": _si(row.get("parsed_voltage_kv")),
        "parsed_from_station": row.get("parsed_from_station"),
        "parsed_to_station": row.get("parsed_to_station"),
        "parsed_single_station": row.get("parsed_single_station"),
        "from_bus_psse": _si(row.get("from_bus_psse")),
        "to_bus_psse": _si(row.get("to_bus_psse")),
        "rating_mva": _sf(row.get("rating_mva")),
        "neighbor_count": _si(row.get("neighbor_count")),
        "match_status": row.get("network_match_status"),
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
        "constraint_name": row.get("constraint_name"),
        "contingency": row.get("contingency"),
        "reported_name": row.get("reported_name"),
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

    funnel_sort = "binding_price_abs" if binding_hours else "total_price"

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
            # Unmatched / interface — keep original total_price ordering.
            sub = df[df["network_match_status"] == status].sort_values(
                "total_price", ascending=False, na_position="last",
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
) -> dict:
    """View model for ``GET /views/constraints_rt_dart_network``.

    Sections produced (sorted by ``ABS(dart_total_price)`` desc, then
    ``rt_total_price`` desc):
      - matched_constraints
      - ambiguous_constraints
      - unmatched_constraints
      - interface_constraints
    """
    if enriched_df is None or enriched_df.empty:
        return {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "match_coverage": _coverage(pd.DataFrame()),
            "matched_constraints": [],
            "ambiguous_constraints": [],
            "unmatched_constraints": [],
            "interface_constraints": [],
        }

    pivoted = _pivot_rt_dart(enriched_df)

    pivoted["dart_abs"] = pd.to_numeric(
        pivoted.get("dart_total_price"), errors="coerce",
    ).abs().fillna(0)
    pivoted["rt_total_price"] = pd.to_numeric(
        pivoted.get("rt_total_price"), errors="coerce",
    )

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
        "match_coverage": _coverage(pivoted),
        **sections,
    }


def _rt_dart_record(
    row: pd.Series, branches_df: pd.DataFrame, *, max_neighbors: int,
) -> dict:
    """Wide row format for the RT+DART pivoted view (2-hop ≥230 kV neighbors)."""
    from backend.mcp_server.data.network_match import k_hop_neighbors

    rec = {
        "date": str(row["date"]) if pd.notna(row.get("date")) else None,
        "constraint_name": row.get("constraint_name"),
        "contingency": row.get("contingency"),
        "reported_name": row.get("reported_name"),
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
