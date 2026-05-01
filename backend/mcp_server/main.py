"""FastAPI MCP entry point — run with: uvicorn backend.mcp_server.main:app --reload

Each endpoint is a thin wrapper:

    GET /views/<endpoint>?format=md|json
        ↓
    data.<module>.pull_*()                — select * from <DBT_SCHEMA>.<mart>
        ↓
    views.<module>.build_*_view_model()
        ↓
    (md) views.markdown_formatters.format_*(vm)
    (json) view-model dict

Endpoint → source mapping:

  Outages:
    /views/transmission_outages_active                → pjm_transmission_outages_active
    /views/transmission_outages_window_7d             → pjm_transmission_outages_window_7d
    /views/transmission_outages_changes_24h_simple    → pjm_transmission_outages_changes_24h_simple
    /views/transmission_outages_changes_24h_snapshot  → pjm_transmission_outages_changes_24h_snapshot
    /views/transmission_outages_network               → active mart + PSS/E network model
  Constraints:
    /views/constraints_da_network                     → pjm_constraints_hourly_pivot (DA, k=2 ≥230kV)
    /views/constraints_rt_dart_network                → pjm_constraints_hourly_pivot (RT+DART, k=2 ≥230kV)
  DA LMPs:
    /views/lmp_da_hub_summary                         → pjm_lmps_hourly (market='da')
    /views/lmp_da_outage_overlap                      → constraints × outages × PSS/E network
"""
import logging
from datetime import date, timedelta
from enum import Enum

import backend.settings  # noqa: F401 — load env vars

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi_mcp import FastApiMCP

from backend.mcp_server.data import constraints, lmp, transmission_outages
from backend.mcp_server.data.constraint_network_match import (
    match_constraints_to_branches,
)
from backend.mcp_server.data.network_match import (
    load_network,
    match_outages_to_branches,
)
from backend.mcp_server.views.constraints import (
    build_da_network_view_model,
    build_rt_dart_network_view_model,
)
from backend.mcp_server.views.lmp import (
    build_lmp_da_hub_summary_view_model,
    build_lmp_da_outage_overlap_view_model,
    build_lmps_daily_summary_view_model,
    build_lmps_hourly_summary_view_model,
)
from backend.mcp_server.views.markdown_formatters import (
    format_constraints_da_network,
    format_constraints_rt_dart_network,
    format_lmp_da_hub_summary,
    format_lmp_da_outage_overlap,
    format_lmps_daily_summary,
    format_lmps_hourly_summary,
    format_transmission_outages_active,
    format_transmission_outages_changes_24h_simple,
    format_transmission_outages_changes_24h_snapshot,
    format_transmission_outages_for_constraints,
    format_transmission_outages_network,
    format_transmission_outages_window_7d,
)
from backend.mcp_server.views.transmission_outages import (
    build_active_view_model,
    build_changes_24h_simple_view_model,
    build_changes_24h_snapshot_view_model,
    build_network_view_model,
    build_outages_for_constraints_view_model,
    build_window_7d_view_model,
)


class OutputFormat(str, Enum):
    md = "md"
    json = "json"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="PJM DA Forecast API",
    description="One endpoint per dbt mart, JSON or Markdown.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ─── Active mart ─────────────────────────────────────────────────────────────


@app.get("/views/transmission_outages_active")
def get_transmission_outages_active(
    format: OutputFormat = Query(OutputFormat.md, description="md (markdown) or json"),
):
    """Currently active or scheduled-and-locked-in outages.

    Filter: outage_state in (Active, Approved), equipment_type in (LINE, XFMR, PS),
    voltage_kv >= 230. Returns a regional summary plus notable individual tickets
    (high-risk / 500kv+ / new / returning).
    """
    df = transmission_outages.pull_active()
    vm = build_active_view_model(df)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_transmission_outages_active(vm),
        media_type="text/markdown",
    )


# ─── Window 7d mart ──────────────────────────────────────────────────────────


@app.get("/views/transmission_outages_window_7d")
def get_transmission_outages_window_7d(
    format: OutputFormat = Query(OutputFormat.md, description="md (markdown) or json"),
):
    """7-day forward outlook — outages overlapping [now, now+7d].

    Includes Received (planned-but-unapproved) tickets alongside Active/Approved.
    Returns a regional summary plus two lists: locked outages (Active/Approved)
    sorted by days-to-return, and planned outages (Received) sorted by start date.
    """
    df = transmission_outages.pull_window_7d()
    vm = build_window_7d_view_model(df)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_transmission_outages_window_7d(vm),
        media_type="text/markdown",
    )


# ─── Changes 24h — simple ────────────────────────────────────────────────────


@app.get("/views/transmission_outages_changes_24h_simple")
def get_transmission_outages_changes_24h_simple(
    format: OutputFormat = Query(OutputFormat.md, description="md (markdown) or json"),
):
    """Last-24h delta (simple variant) — NEW + REVISED tickets.

    Driven by source-table created_at and last_revised. Useful from day 1.
    Trade-off vs the snapshot variant: no diff text, no CLEARED detection.
    """
    df = transmission_outages.pull_changes_24h_simple()
    vm = build_changes_24h_simple_view_model(df)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_transmission_outages_changes_24h_simple(vm),
        media_type="text/markdown",
    )


# ─── Changes 24h — snapshot ──────────────────────────────────────────────────
# DISABLED 2026-05-01 — letting the SCD2 snapshot accumulate history before
# exposing this view. On day 1 it reports the entire active set as NEW because
# every ticket got the same `dbt_valid_from` at baseline.
#
# The dbt mart and `dbt snapshot` step in the Prefect flow keep running daily
# so history builds up. Re-enable by replacing the endpoint body below with the
# pull/build/format pipeline (see git history for the working version).
#
# Re-enable target: ~2026-05-08 (after 1 week of daily snapshots).


@app.get("/views/transmission_outages_changes_24h_snapshot")
def get_transmission_outages_changes_24h_snapshot(
    format: OutputFormat = Query(OutputFormat.md, description="md (markdown) or json"),
):
    """[DISABLED] Snapshot variant — paused while SCD2 history builds up.

    Use ``/views/transmission_outages_changes_24h_simple`` in the meantime;
    it gives NEW + REVISED from day 1 (no diff_text, no CLEARED).
    """
    msg = (
        "Snapshot variant is disabled while the SCD2 snapshot accumulates "
        "24h+ of history. Use /views/transmission_outages_changes_24h_simple "
        "in the meantime."
    )
    if format == OutputFormat.json:
        return PlainTextResponse(
            content=f'{{"status": "disabled", "message": "{msg}"}}',
            status_code=503,
            media_type="application/json",
        )
    return PlainTextResponse(
        content=f"# Disabled\n\n{msg}\n",
        status_code=503,
        media_type="text/markdown",
    )


# ─── Network-enriched view ───────────────────────────────────────────────────
# The PSS/E parquets (~800 KB combined) are loaded lazily on first request and
# cached in this module. Reset _NETWORK_CACHE to None to force a reload after
# rerunning parse_psse_raw.
_NETWORK_CACHE: tuple[pd.DataFrame, pd.DataFrame] | None = None


def _get_network() -> tuple[pd.DataFrame, pd.DataFrame]:
    global _NETWORK_CACHE
    if _NETWORK_CACHE is None:
        _NETWORK_CACHE = load_network()
        logger.info("loaded PSS/E network parquets into memory")
    return _NETWORK_CACHE


@app.get("/views/transmission_outages_network")
def get_transmission_outages_network(
    format: OutputFormat = Query(OutputFormat.md, description="md (markdown) or json"),
    max_neighbors: int = Query(5, ge=0, le=20, description="1-hop neighbors per outage"),
):
    """Active outages cross-referenced with the PJM PSS/E network model.

    Each outage's facility name is matched to a PSS/E branch by substation
    endpoints + voltage_kv. Matched outages get from-bus/to-bus IDs, MVA
    rating, and a list of 1-hop neighbor branches sharing either endpoint
    substation. Outages are bucketed by match status:
      - matched   : exactly one PSS/E candidate
      - ambiguous : multiple candidates (typically multi-XFMR substations)
      - unmatched : no candidate (substation missing from PSS/E or
                    non-standard description)
    """
    buses_df, branches_df = _get_network()
    active_df = transmission_outages.pull_active()
    enriched = match_outages_to_branches(active_df, branches_df, buses_df)
    vm = build_network_view_model(enriched, branches_df, max_neighbors=max_neighbors)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_transmission_outages_network(vm),
        media_type="text/markdown",
    )


# ─── Binding constraints — DA forward view ───────────────────────────────────


@app.get("/views/constraints_da_network")
def get_constraints_da_network(
    target_date: date | None = Query(
        None, description="DA target date (default: tomorrow)",
    ),
    top_n: int = Query(
        20, ge=1, le=200, description="Top-N constraints by total_price",
    ),
    max_neighbors: int = Query(
        10, ge=0, le=30, description="2-hop ≥230kV neighbors per matched constraint",
    ),
    binding_hours: list[int] | None = Query(
        None, description="HE values 1-24 from Tier 2; when set, filters and re-ranks",
    ),
    format: OutputFormat = Query(OutputFormat.md, description="md or json"),
):
    """DA binding constraints for a target date, cross-referenced with PSS/E.

    Forward-looking: defaults to tomorrow's DA results (use after the DA
    market clears, ~13:30 EPT). Each constraint's ``monitored_facility`` is
    parsed (DA-coded / RT-EMS / prose-l/o / interface) and matched to a
    PSS/E branch by station + voltage. Neighbors are 2-hop, ≥230 kV
    (parallel-path topology around the seed branch). Sections:
    matched / ambiguous / unmatched / interface.

    When ``binding_hours`` is supplied (Tier 3 funnel mode), matched and
    ambiguous constraints are re-ranked by sum-over-binding-hours shadow
    price, ``hourly`` collapses to ``hourly_binding`` (only the binding
    HEs), and ``neighbors`` is replaced by a flat ``neighbor_bus_ids``
    list for downstream Tier 4 cross-linking. Without the param, the
    response shape is unchanged for backward compat.
    """
    if target_date is None:
        target_date = date.today() + timedelta(days=1)

    buses_df, branches_df = _get_network()
    df = constraints.pull_constraints_da(target_date)
    enriched = match_constraints_to_branches(df, branches_df, buses_df)
    vm = build_da_network_view_model(
        enriched, branches_df, target_date,
        top_n=top_n, max_neighbors=max_neighbors,
        binding_hours=binding_hours,
    )
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_constraints_da_network(vm),
        media_type="text/markdown",
    )


# ─── Binding constraints — RT + DART backward view ───────────────────────────


@app.get("/views/constraints_rt_dart_network")
def get_constraints_rt_dart_network(
    start_date: date | None = Query(
        None, description="Window start (default: T-7)",
    ),
    end_date: date | None = Query(
        None, description="Window end (default: T-1)",
    ),
    top_n: int = Query(
        30, ge=1, le=300, description="Top-N (date, constraint) pairs by |DART|",
    ),
    max_neighbors: int = Query(
        10, ge=0, le=30, description="2-hop ≥230kV neighbors per matched constraint",
    ),
    format: OutputFormat = Query(OutputFormat.md, description="md or json"),
):
    """RT and DART binding constraints over a window, pivoted side-by-side.

    Backward-looking: each row is one ``(date, constraint, contingency)``
    pair carrying both ``rt_*`` and ``dart_*`` totals. DART rows only exist
    when the same constraint bound in BOTH DA and RT that day. Sorted by
    ``|dart_total_price|`` desc so the largest DA→RT spreads surface first.
    """
    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    if start_date is None:
        start_date = end_date - timedelta(days=6)

    buses_df, branches_df = _get_network()
    df = constraints.pull_constraints_rt_dart(start_date, end_date)
    enriched = match_constraints_to_branches(df, branches_df, buses_df)
    vm = build_rt_dart_network_view_model(
        enriched, branches_df, start_date, end_date,
        top_n=top_n, max_neighbors=max_neighbors,
    )
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_constraints_rt_dart_network(vm),
        media_type="text/markdown",
    )


# ─── DA LMP — hub summary ────────────────────────────────────────────────────


@app.get("/views/lmp_da_hub_summary")
def get_lmp_da_hub_summary(
    target_date: date | None = Query(
        None, description="DA target date (default: tomorrow)",
    ),
    format: OutputFormat = Query(OutputFormat.md, description="md or json"),
):
    """Hub-level DA LMP decomposition for a target date.

    One row per hub: total / energy / congestion / loss split into onpeak
    (HE 8-23), offpeak, flat, plus the day's peak hour. Sorted by
    |onpeak congestion|. Header carries market-wide averages and a count
    of hubs where onpeak |congestion| / |total| exceeds 10% — a quick
    signal of network stress before drilling into specific constraints.

    DA-only because the upstream scrape filters ``type=hub``; only PJM
    aggregate hubs (~15-20) are in the mart, no zonal or bus-level LMP.
    """
    if target_date is None:
        target_date = date.today() + timedelta(days=1)

    df = lmp.pull_lmp_da_hourly(target_date)
    vm = build_lmp_da_hub_summary_view_model(df, target_date)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_lmp_da_hub_summary(vm),
        media_type="text/markdown",
    )


# ─── DA LMP — constraint × outage overlap ────────────────────────────────────


@app.get("/views/lmp_da_outage_overlap")
def get_lmp_da_outage_overlap(
    target_date: date | None = Query(
        None, description="DA target date (default: tomorrow)",
    ),
    top_n: int = Query(
        20, ge=1, le=100, description="Top-N DA constraints by total_price",
    ),
    max_neighbors: int = Query(
        10, ge=0, le=30, description="2-hop ≥230kV neighbors per constraint",
    ),
    format: OutputFormat = Query(OutputFormat.md, description="md or json"),
):
    """Top binding DA constraints crossed against transmission outages.

    For each top-N matched constraint: expand to its 2-hop ≥230 kV neighbor
    set on the PSS/E network, then look for transmission outages in
    [target_date, target_date + 7d] that sit on the seed branch or any
    neighbor. Outages bucketed Active / Starting soon / Ending soon.

    Window source: ``pjm_transmission_outages_window_7d`` (covers the
    upcoming 7 days from the dbt run, may miss 1-2 days of horizon if the
    target_date is far in the future relative to the last refresh).
    """
    if target_date is None:
        target_date = date.today() + timedelta(days=1)

    buses_df, branches_df = _get_network()

    constraints_df = constraints.pull_constraints_da(target_date)
    enriched_constraints = match_constraints_to_branches(
        constraints_df, branches_df, buses_df,
    )

    outages_df = transmission_outages.pull_window_7d()
    enriched_outages = match_outages_to_branches(outages_df, branches_df, buses_df)

    vm = build_lmp_da_outage_overlap_view_model(
        enriched_constraints, enriched_outages, branches_df, target_date,
        top_n=top_n, max_neighbors=max_neighbors,
    )
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_lmp_da_outage_overlap(vm),
        media_type="text/markdown",
    )


# ─── DA results brief — Tier 1: daily LMP summary ────────────────────────────


@app.get("/views/lmps_daily_summary")
def get_lmps_daily_summary(
    target_date: date | None = Query(
        None, description="DA target date (default: tomorrow)",
    ),
    top_n_drilldown: int = Query(
        5, ge=1, le=20, description="Hubs handed to Tier 2 hourly drilldown",
    ),
    format: OutputFormat = Query(OutputFormat.md, description="md or json"),
):
    """Tier 1 of the DA-results funnel — zonal/hub daily summary.

    Hub-grain (mart only carries PJM aggregate hubs, not zonal). One row
    per hub: total / energy / congestion / loss split into onpeak / offpeak.
    Surfaces ``top_zones_for_drilldown`` — the top-N hubs by absolute
    onpeak congestion that Tier 2 reads as its hub filter.
    """
    if target_date is None:
        target_date = date.today() + timedelta(days=1)

    df = lmp.pull_lmp_da_hourly(target_date)
    vm = build_lmps_daily_summary_view_model(
        df, target_date, top_n_drilldown=top_n_drilldown,
    )
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_lmps_daily_summary(vm),
        media_type="text/markdown",
    )


# ─── DA results brief — Tier 2: hourly LMP drilldown ─────────────────────────


@app.get("/views/lmps_hourly_summary")
def get_lmps_hourly_summary(
    target_date: date | None = Query(
        None, description="DA target date (default: tomorrow)",
    ),
    hubs: str | None = Query(
        None, description="Comma-separated hub names (default: top 5 from Tier 1)",
    ),
    binding_threshold: float = Query(
        25.0, ge=0, le=500,
        description="$/MWh — hours where any hub crosses this become 'binding'",
    ),
    format: OutputFormat = Query(OutputFormat.md, description="md or json"),
):
    """Tier 2 of the DA-results funnel — hourly LMP drilldown.

    Filtered to a small hub set (Tier 1 hands the top-5 list). Renders an
    hour × hub congestion heatmap and surfaces
    ``binding_hours_for_drilldown`` — 3-5 HEs where congestion crossed
    ``binding_threshold``. Tier 3 reads that field to filter constraints.
    """
    if target_date is None:
        target_date = date.today() + timedelta(days=1)

    hubs_list = [h.strip() for h in hubs.split(",")] if hubs else None
    df = lmp.pull_lmp_da_hourly(target_date, hubs=hubs_list)
    vm = build_lmps_hourly_summary_view_model(
        df, target_date,
        hubs_filter=hubs_list, binding_threshold=binding_threshold,
    )
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_lmps_hourly_summary(vm),
        media_type="text/markdown",
    )


# ─── DA results brief — Tier 4: outages on/near constraint buses ─────────────


def _parse_int_csv(s: str | None) -> list[int]:
    """Parse a CSV of ints, raising ValueError on bad tokens."""
    if not s:
        return []
    out: list[int] = []
    for tok in s.split(","):
        tok = tok.strip()
        if not tok:
            continue
        out.append(int(tok))
    return out


def _parse_constraint_labels(s: str | None) -> dict[int, list[str]]:
    """Parse 'bus:label,bus:label,...' into a bus→labels mapping.

    Multiple labels for the same bus are accumulated.
    """
    if not s:
        return {}
    out: dict[int, list[str]] = {}
    for tok in s.split(","):
        tok = tok.strip()
        if not tok or ":" not in tok:
            continue
        bus_str, _, label = tok.partition(":")
        try:
            bus = int(bus_str.strip())
        except ValueError:
            continue
        out.setdefault(bus, []).append(label.strip())
    return out


@app.get("/views/transmission_outages_for_constraints")
def get_transmission_outages_for_constraints(
    bus_ids: str = Query(
        ..., description="CSV of PSS/E bus integers (Tier 3 neighbor_bus_ids union)",
    ),
    constraint_labels: str | None = Query(
        None,
        description="CSV 'bus:label,bus:label' for cross-link annotation",
    ),
    max_neighbors: int = Query(
        3, ge=0, le=10, description="1-hop neighbors per outage",
    ),
    format: OutputFormat = Query(OutputFormat.md, description="md or json"),
):
    """Tier 4 of the DA-results funnel — outages near constraint buses.

    Filters active outages to those whose ``from_bus_psse`` or
    ``to_bus_psse`` is in the supplied ``bus_ids`` set. When
    ``constraint_labels`` is supplied, each outage row is annotated with
    the binding constraint(s) it sits near — the visible cross-link
    between price (Tier 3) and physical cause (this tier).
    """
    try:
        bus_id_list = _parse_int_csv(bus_ids)
    except ValueError:
        return PlainTextResponse(
            content='{"error": "bus_ids must be a CSV of integers"}',
            status_code=400,
            media_type="application/json",
        )
    if not bus_id_list:
        return PlainTextResponse(
            content='{"error": "bus_ids is required"}',
            status_code=400,
            media_type="application/json",
        )

    label_map = _parse_constraint_labels(constraint_labels)

    buses_df, branches_df = _get_network()
    active_df = transmission_outages.pull_active()
    enriched = match_outages_to_branches(active_df, branches_df, buses_df)
    vm = build_outages_for_constraints_view_model(
        enriched, branches_df, bus_id_list,
        constraint_index=label_map,
        max_neighbors=max_neighbors,
    )
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(
        content=format_transmission_outages_for_constraints(vm),
        media_type="text/markdown",
    )


# ─── MCP integration — exposes all endpoints as agent tools ──────────────────
mcp = FastApiMCP(app)
mcp.mount_http()
