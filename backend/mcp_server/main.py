"""FastAPI MCP entry point — run with: uvicorn backend.mcp_server.main:app --reload

Exposes one HTTP endpoint per dbt mart in the transmission-outages family.
Each endpoint is a thin wrapper:

    GET /views/<endpoint>?format=md|json
        ↓
    data.transmission_outages.pull_<mart>()       — select * from <DBT_SCHEMA>.<mart>
        ↓
    views.transmission_outages.build_<mart>_view_model(df)
        ↓
    (md) views.markdown_formatters.format_<mart>(vm)
    (json) view-model dict

Endpoint → mart mapping:

    /views/transmission_outages_active                → pjm_transmission_outages_active
    /views/transmission_outages_window_7d             → pjm_transmission_outages_window_7d
    /views/transmission_outages_changes_24h_simple    → pjm_transmission_outages_changes_24h_simple
    /views/transmission_outages_changes_24h_snapshot  → pjm_transmission_outages_changes_24h_snapshot
"""
import logging
from enum import Enum

import backend.settings  # noqa: F401 — load env vars

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi_mcp import FastApiMCP

from backend.mcp_server.data import transmission_outages
from backend.mcp_server.views.markdown_formatters import (
    format_transmission_outages_active,
    format_transmission_outages_changes_24h_simple,
    format_transmission_outages_changes_24h_snapshot,
    format_transmission_outages_window_7d,
)
from backend.mcp_server.views.transmission_outages import (
    build_active_view_model,
    build_changes_24h_simple_view_model,
    build_changes_24h_snapshot_view_model,
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


# ─── MCP integration — exposes all endpoints as agent tools ──────────────────
mcp = FastApiMCP(app)
mcp.mount_http()
