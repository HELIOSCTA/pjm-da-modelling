"""Canonical tall-parquet schema for backtest results.

One row per (target_date, hour_ending, model_name). Every model's
adapter (in ``registry.py``) collapses its idiosyncratic output frame
into rows that match the columns below. Missing quantile levels (e.g.
``baseline_meteo`` only has 3 effective bands -- ENS Bottom / Avg / Top)
land as NaN, which the metrics code handles gracefully.

Schema is locked: column names are the public contract that the
``replay``, ``metrics``, ``pdc``, and ``leaderboard`` modules depend
on. Add new columns to the OPTIONAL block (NaN where not applicable)
rather than renaming any of the required ones.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

# Required columns -- present on every row.
REQUIRED_COLUMNS: tuple[str, ...] = (
    "target_date",
    "hour_ending",
    "model_name",
    "actual_lmp",  # settled DA LMP at HUB; NaN when not yet settled
    "point",  # model's point forecast ($/MWh)
    "q_0.10",
    "q_0.25",
    "q_0.50",
    "q_0.75",
    "q_0.90",
)

# Optional family-specific columns. Adapters populate the ones their
# model produces; the rest are NaN by construction. Add new ones here.
OPTIONAL_COLUMNS: tuple[str, ...] = (
    "marginal_fuel",  # supply_stack: e.g. "Gas CC" / "Coal" / "Gas CT/ST"
    "marginal_heat_rate",  # supply_stack
    "utilization",  # supply_stack
    "scarcity_adder",  # supply_stack
    "bid_markup",  # supply_stack
    "backward_coef_share",  # linear_arx: 0 when backward-LMP anchor is off
)

ALL_COLUMNS: tuple[str, ...] = REQUIRED_COLUMNS + OPTIONAL_COLUMNS


def empty_frame() -> pd.DataFrame:
    """A zero-row DataFrame with the full schema -- useful for early returns."""
    return pd.DataFrame({c: pd.Series(dtype="object") for c in ALL_COLUMNS})


def build_row(
    *,
    target_date: date,
    hour_ending: int,
    model_name: str,
    actual_lmp: float | None = None,
    point: float | None = None,
    quantiles: dict[float, float] | None = None,
    extras: dict[str, object] | None = None,
) -> dict:
    """Build one canonical-schema row. ``quantiles`` is keyed by the
    fraction (0.10 .. 0.90); ``extras`` is any subset of OPTIONAL_COLUMNS."""
    row: dict[str, object] = {
        "target_date": target_date,
        "hour_ending": int(hour_ending),
        "model_name": str(model_name),
        "actual_lmp": float(actual_lmp) if actual_lmp is not None else float("nan"),
        "point": float(point) if point is not None else float("nan"),
    }
    qs = quantiles or {}
    for q in (0.10, 0.25, 0.50, 0.75, 0.90):
        v = qs.get(q)
        row[f"q_{q:.2f}"] = float(v) if v is not None else float("nan")
    for col in OPTIONAL_COLUMNS:
        row[col] = (extras or {}).get(col, None)
    return row


def assert_schema(df: pd.DataFrame) -> None:
    """Cheap run-time guard: every REQUIRED column is present."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Backtest frame missing required columns: {missing}")
