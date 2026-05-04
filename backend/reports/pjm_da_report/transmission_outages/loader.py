"""Transmission outages loader — pulls active mart and overlays PSS/E network match.

Single Postgres pull (`pjm_transmission_outages_active` mart) + a bus-id
enrichment step. Returns the enriched DataFrame plus the branches DataFrame
the network view-model needs to resolve k-hop neighbors.

There's no parquet cache for transmission outages — the marts are Postgres-only.
"""
from __future__ import annotations

import pandas as pd

from backend.mcp_server.data.network_match import (
    load_network,
    match_outages_to_branches,
)
from backend.mcp_server.data.transmission_outages import pull_active


def load_active_outages() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(enriched_outages_df, branches_df)``.

    The enriched DataFrame carries PSS/E ``from_bus_psse`` / ``to_bus_psse`` /
    ``rating_mva`` / ``network_match_status`` / ``neighbor_count`` columns.
    Returns an empty frame (not a raise) when the mart is empty so the
    report can render a friendly status fragment instead of crashing.
    """
    raw = pull_active()
    buses, branches = load_network()
    if raw is None or raw.empty:
        return raw if raw is not None else pd.DataFrame(), branches
    enriched = match_outages_to_branches(raw, branches, buses)
    return enriched, branches
