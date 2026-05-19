"""Model registry for the cross-model backtest.

Each entry pairs a family's ``run(...)`` callable with an *adapter* that
converts the return dict into canonical-schema rows
(``schemas.build_row``). Adding a new family to the backtest = one
``ModelEntry`` here; everything downstream (replay, metrics, leaderboard)
keys off ``model_name`` and the tall-parquet schema, so it picks up the
new family automatically.

Adapter contract: ``adapt(result_dict, target_date, model_name) ->
list[dict]``, one dict per (target_date, hour_ending) in canonical
schema. The adapter is responsible for mapping the family's quantile
naming (e.g. ``q_0.10`` / ``ENS Bottom`` / ``P10``) into the canonical
``quantiles`` dict.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable

import pandas as pd

from da_models.backtest.schemas import build_row

logger = logging.getLogger(__name__)

# Adapter signature: (result_dict, target_date, model_name) -> list[row-dict]
Adapter = Callable[[dict, date, str], list[dict]]


@dataclass
class ModelEntry:
    name: str
    run: Callable[..., dict]
    adapt: Adapter
    run_kwargs: dict[str, Any] = field(default_factory=dict)


# ── Adapters ───────────────────────────────────────────────────────────────
_Q_COLS_LINEAR_ARX = ("q_0.10", "q_0.25", "q_0.50", "q_0.75", "q_0.90")


def _resolve_forecast_frame(result: dict) -> pd.DataFrame | None:
    """Resolve the family's per-HE forecast frame. ``supply_stack`` keys it
    as ``forecast_table``; ``linear_arx`` (via ``run.py::run_single_day``)
    keys it as ``df_forecast``. Structurally identical frames -- just a
    naming drift between the two families."""
    fc = result.get("forecast_table")
    if fc is not None and len(fc) > 0:
        return fc
    return result.get("df_forecast")


def _adapt_canonical_q_cols(
    result: dict, target_date: date, model_name: str
) -> list[dict]:
    """Adapter for families that already emit ``hour_ending`` + ``point_forecast``
    + ``q_0.10``..``q_0.90`` (linear_arx variants, supply_stack)."""
    fc = _resolve_forecast_frame(result)
    if fc is None or len(fc) == 0:
        logger.warning(
            "%s @ %s: empty forecast frame (no forecast_table / df_forecast)",
            model_name,
            target_date,
        )
        return []
    out: list[dict] = []
    for _, r in fc.iterrows():
        he = r.get("hour_ending")
        if pd.isna(he):
            continue
        # ``clearing_price`` (supply_stack) and ``point_forecast`` (linear_arx)
        # are equivalent point columns; prefer point_forecast when both exist.
        point = r.get("point_forecast")
        if pd.isna(point):
            point = r.get("clearing_price")
        quantiles = {
            q: float(r[c])
            for q, c in [
                (0.10, "q_0.10"),
                (0.25, "q_0.25"),
                (0.50, "q_0.50"),
                (0.75, "q_0.75"),
                (0.90, "q_0.90"),
            ]
            if c in fc.columns and pd.notna(r.get(c))
        }
        extras = {
            k: r.get(k)
            for k in (
                "marginal_fuel",
                "marginal_heat_rate",
                "utilization",
                "scarcity_adder",
                "bid_markup",
            )
            if k in fc.columns and pd.notna(r.get(k))
        }
        if (
            "backward_coef_share" in result
            and result["backward_coef_share"] is not None
        ):
            extras["backward_coef_share"] = float(result["backward_coef_share"])
        out.append(
            build_row(
                target_date=target_date,
                hour_ending=int(he),
                model_name=model_name,
                point=float(point) if pd.notna(point) else None,
                quantiles=quantiles,
                extras=extras,
            )
        )
    return out


def _adapt_baseline_meteo(
    result: dict, target_date: date, model_name: str
) -> list[dict]:
    """Adapter for ``baseline_meteo_da_price``. Its ``bands_table`` rows are
    keyed by ``Type`` (Det / ENS Avg / ENS Bottom / ENS Top) and columns are
    HE1..HE24 + OnPeak/OffPeak/Flat. Map ENS Avg -> point + q_0.50,
    ENS Bottom -> q_0.10, ENS Top -> q_0.90. ``q_0.25`` and ``q_0.75``
    stay NaN (Meteologica doesn't expose the IQR knots)."""
    bands = result.get("bands_table")
    if bands is None or len(bands) == 0:
        logger.warning("%s @ %s: empty bands_table", model_name, target_date)
        return []

    def _row(name: str) -> pd.Series | None:
        sel = bands[bands["Type"] == name]
        return sel.iloc[0] if len(sel) else None

    avg = _row("ENS Avg")
    bot = _row("ENS Bottom")
    top = _row("ENS Top")
    if avg is None:
        avg = _row("Det")  # fall back to deterministic if ENS is missing
    if avg is None:
        return []
    out: list[dict] = []
    for h in range(1, 25):
        col = f"HE{h}"
        point = avg.get(col)
        if pd.isna(point):
            continue
        q = {0.50: float(point)}
        if bot is not None and pd.notna(bot.get(col)):
            q[0.10] = float(bot[col])
        if top is not None and pd.notna(top.get(col)):
            q[0.90] = float(top[col])
        out.append(
            build_row(
                target_date=target_date,
                hour_ending=h,
                model_name=model_name,
                point=float(point),
                quantiles=q,
            )
        )
    return out


# ── Registry build ─────────────────────────────────────────────────────────
def _make_registry() -> dict[str, ModelEntry]:
    """Lazy import + assembly so a broken family doesn't poison the module."""
    reg: dict[str, ModelEntry] = {}

    # supply_stack: latest_only=False so the lead-1 historical vintage is used
    # (matches how the model would have run on the actual D-1).
    try:
        from da_models.supply_stack.pipelines.forecast_single_day import run as ss_run

        reg["supply_stack"] = ModelEntry(
            name="supply_stack",
            run=ss_run,
            adapt=_adapt_canonical_q_cols,
            run_kwargs={"latest_only": False, "with_bands": True},
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("supply_stack registration failed: %s", exc)

    # linear_arx -- pjm_hourly variant.
    try:
        from da_models.linear_arx_da_price.pjm_hourly.pipelines.forecast_single_day import (
            run as lapjm_run,
        )

        reg["linear_arx_pjm_hourly"] = ModelEntry(
            name="linear_arx_pjm_hourly",
            run=lapjm_run,
            adapt=_adapt_canonical_q_cols,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("linear_arx_pjm_hourly registration failed: %s", exc)

    # linear_arx -- meteo_hourly variant.
    try:
        from da_models.linear_arx_da_price.meteo_hourly.pipelines.forecast_single_day import (
            run as lameteo_run,
        )

        reg["linear_arx_meteo_hourly"] = ModelEntry(
            name="linear_arx_meteo_hourly",
            run=lameteo_run,
            adapt=_adapt_canonical_q_cols,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("linear_arx_meteo_hourly registration failed: %s", exc)

    # baseline_meteo: the comparison anchor.
    try:
        from da_models.baseline_meteo_da_price.pipelines.forecast_single_day import (
            run as bm_run,
        )

        reg["baseline_meteo"] = ModelEntry(
            name="baseline_meteo",
            run=bm_run,
            adapt=_adapt_baseline_meteo,
            run_kwargs={"lead_days": 1},
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("baseline_meteo registration failed: %s", exc)

    return reg


REGISTRY: dict[str, ModelEntry] = _make_registry()


def known_models() -> list[str]:
    return sorted(REGISTRY)
