"""``pjm_hourly`` variant config -- feature-source-specific knobs.

Estimator / calibration-window / band constants are shared, in
``da_models.linear_arx_da_price.configs``.
"""

from __future__ import annotations

VARIANT: str = "pjm_hourly"
MODEL_NAME: str = "linear_arx_da_price_pjm_hourly_western_hub"
DESCRIPTION: str = (
    "LEAR-style linear ARX -- 24 per-hour Ridge regressions on PJM forward "
    "fundamentals (RTO supply-demand + sub-zonal load)"
)
DEMAND_BLOCK_LABEL: str = (
    "PJM: RTO load/solar/wind/net-load forecast + sub-zonal load (MIDATL/WEST/SOUTH)"
)

# Demand-block feeds.
LOAD_RTO_REGION: str = "RTO"
SUBZONE_LOAD_REGIONS: tuple[str, ...] = (
    "MIDATL",
    "WEST",
    "SOUTH",
)  # never drop SOUTH a priori
PRIMARY_LOAD_COL: str = "load_rto"  # drives daily aggregates + load_sq / load_x_gas
PRIMARY_NET_LOAD_COL: str = "net_load_rto"  # drives net_load_sq / net_load_x_gas
PRIMARY_GAS_COL: str = "gas_m3"
TARGET_REQUIRED_COLS: tuple[str, ...] = ("load_rto",)

# Backward (reference-day realized DA-LMP) anchors. Default off -- forward-only
# (see backward_vs_forward_looking.md); flip True to add the capped anchor.
INCLUDE_BACKWARD_LMP: bool = False
BACKWARD_LMP_DEFAULT_LAG_DAYS: int = 1
BACKWARD_LMP_MONDAY_LAG_DAYS: int = 3
