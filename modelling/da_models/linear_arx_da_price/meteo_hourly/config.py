"""``meteo_hourly`` variant config -- feature-source-specific knobs.

Estimator / calibration-window / band constants are shared, in
``da_models.linear_arx_da_price.configs``.
"""

from __future__ import annotations

VARIANT: str = "meteo_hourly"
MODEL_NAME: str = "linear_arx_da_price_meteo_hourly_western_hub"
DESCRIPTION: str = (
    "LEAR-style linear ARX -- 24 per-hour Ridge regressions on Meteologica "
    "regional supply-demand (load/solar/wind/net-load for RTO + MIDATL/WEST/SOUTH)"
)
DEMAND_BLOCK_LABEL: str = (
    "Meteologica: load/solar/wind/net-load forecast x {RTO, MIDATL, WEST, SOUTH}"
)

# Demand-block feeds: Meteologica supply-demand for these regions.
# 'SOUTH' (Dominion) stays in by default -- never drop a priori.
METEO_REGIONS: tuple[str, ...] = ("RTO", "MIDATL", "WEST", "SOUTH")
METEO_SERIES: tuple[str, ...] = ("load", "solar", "wind", "net_load")
PRIMARY_LOAD_COL: str = (
    "meteo_load_rto"  # drives daily aggregates + load_sq / load_x_gas
)
PRIMARY_NET_LOAD_COL: str = "meteo_net_load_rto"  # drives net_load_sq / net_load_x_gas
PRIMARY_GAS_COL: str = "gas_m3"
TARGET_REQUIRED_COLS: tuple[str, ...] = ("meteo_load_rto",)

# Backward (reference-day realized DA-LMP) anchors. Default off -- forward-only
# (see backward_vs_forward_looking.md); flip True to add the capped anchor.
INCLUDE_BACKWARD_LMP: bool = False
BACKWARD_LMP_DEFAULT_LAG_DAYS: int = 1
BACKWARD_LMP_MONDAY_LAG_DAYS: int = 3
