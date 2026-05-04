"""Constants for the DA LMP report bundle."""
from __future__ import annotations

# Hubs are split into "primary" pricing hubs (the ones traders watch) and
# "gen" hubs (generation-side aggregates). Western Hub is first by trader
# convention; flip REPORT_HUBS to HUBS_ALL to also surface gen hubs.
HUBS_PRIMARY = [
    "WESTERN HUB",
    "EASTERN HUB",
    "AEP-DAYTON HUB",
    "DOMINION HUB",
    "NEW JERSEY HUB",
    "CHICAGO HUB",
    "OHIO HUB",
    "N ILLINOIS HUB",
]
HUBS_GEN = ["AEP GEN HUB", "ATSI GEN HUB", "CHICAGO GEN HUB", "WEST INT HUB"]
HUBS_ALL = HUBS_PRIMARY + HUBS_GEN

# Hubs rendered in the report, in order. HUBS_ALL surfaces the 4 gen-side
# aggregates (AEP GEN / ATSI GEN / CHICAGO GEN / WEST INT) alongside the 8
# trader pricing hubs.
REPORT_HUBS = HUBS_ALL

PLOTLY_TEMPLATE = "plotly_dark"

# Component colors — used for chart lines (3-trace per-hub chart).
TOTAL_COLOR = "#60a5fa"       # blue
SYSTEM_COLOR = "#facc15"      # yellow
CONGESTION_COLOR = "#f87171"  # red

# Hub palette — distinct, readable on dark background. Used for the
# Summary section's multi-hub overlay charts.
HUB_COLORS: dict[str, str] = {
    "WESTERN HUB":     "#60a5fa",
    "EASTERN HUB":     "#fbbf24",
    "AEP-DAYTON HUB":  "#34d399",
    "DOMINION HUB":    "#a78bfa",
    "NEW JERSEY HUB":  "#f87171",
    "CHICAGO HUB":     "#fb923c",
    "OHIO HUB":        "#22d3ee",
    "N ILLINOIS HUB":  "#facc15",
    "WEST INT HUB":    "#94a3b8",
    "AEP GEN HUB":     "#10b981",
    "ATSI GEN HUB":    "#c084fc",
    "CHICAGO GEN HUB": "#f97316",
}
