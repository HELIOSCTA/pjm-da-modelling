"""FastAPI MCP entry point — run with: uvicorn backend.mcp_server.main:app --reload

Serves view model endpoints for agent and frontend consumption.
"""
import logging
from enum import Enum

import backend.settings  # noqa: F401 — load env vars

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi_mcp import FastApiMCP

from backend.mcp_server.data import transmission_outages
from backend.mcp_server.views.transmission_outages import build_view_model as tx_outage_view_model
from backend.mcp_server.views.markdown_formatters import format_transmission_outages


class OutputFormat(str, Enum):
    md = "md"
    json = "json"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="PJM DA Forecast API",
    description="Structured view models for like-day forecast data",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/views/transmission_outages")
def get_transmission_outages(
    format: OutputFormat = Query(OutputFormat.md, description="Response format: md (markdown) or json"),
):
    """Return active transmission outages: regional summary + notable individual outages."""
    df = transmission_outages.pull()
    vm = tx_outage_view_model(df)
    if format == OutputFormat.json:
        return vm
    return PlainTextResponse(content=format_transmission_outages(vm), media_type="text/markdown")


# ── MCP integration — exposes all endpoints as agent tools ──────────
mcp = FastApiMCP(app)
mcp.mount_http()
