"""Replay loop: for each (model_name, target_date), call the family's
``run(quiet=True)``, adapt the return dict to canonical rows, join to
settled DA LMP at the hub, and stack into one tall DataFrame.

Slow by construction -- the supply-stack family does Monte-Carlo bands
and takes ~10-15s/day on a ~2,800-unit fleet, linear_arx is a few
seconds, baseline_meteo is sub-second. A 7-day x 4-model run lands in
roughly 5 minutes serial. Parallelisation (per (date, model) cell) is a
future enhancement; for the v1 7-day window it's not needed.

Soft-fail semantics: a single (model, date) cell that raises is logged
and skipped; the loop continues. The leaderboard handles missing cells
gracefully (drops rows with NaN ``point`` from per-model metrics).
"""

from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path

import pandas as pd

from da_models.backtest import configs as C
from da_models.backtest.registry import REGISTRY, ModelEntry
from da_models.backtest.schemas import ALL_COLUMNS, assert_schema
from da_models.common.data import loader
from da_models.common.data.lmp_pool import build_lmp_labels
from da_models.common.forecast.output import actuals_from_pool

logger = logging.getLogger(__name__)


def _settled_actuals(
    target_date: date, hub: str, cache_dir: Path | None
) -> dict[int, float] | None:
    """Per-HE settled DA LMP at ``hub`` for ``target_date``, or None if not yet settled."""
    try:
        label_wide = build_lmp_labels(loader.load_lmps_da(cache_dir=cache_dir), hub)
    except Exception as exc:  # noqa: BLE001
        logger.warning("could not load settled DA LMP for %s: %s", target_date, exc)
        return None
    return actuals_from_pool(label_wide, target_date)


def replay_one(
    entry: ModelEntry, target_date: date, *, hub: str, cache_dir: Path | None
) -> list[dict]:
    """Call one family's ``run`` for one date, adapt, join to actuals.

    Returns canonical-schema row dicts (empty list on failure)."""
    t0 = time.time()
    try:
        result = entry.run(target_date=target_date, quiet=True, **entry.run_kwargs)
    except Exception as exc:  # noqa: BLE001
        logger.exception("%s @ %s failed: %s", entry.name, target_date, exc)
        return []
    rows = entry.adapt(result, target_date, entry.name)
    actuals = _settled_actuals(target_date, hub, cache_dir)
    if actuals is not None:
        for r in rows:
            r["actual_lmp"] = actuals.get(int(r["hour_ending"]), float("nan"))
    elapsed = time.time() - t0
    logger.info(
        "%s @ %s: %d HE rows  (%.1fs)", entry.name, target_date, len(rows), elapsed
    )
    return rows


def replay_grid(
    target_dates: list[date],
    model_names: list[str] | None = None,
    *,
    hub: str = C.HUB,
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Replay every (model, date) cell. Returns one tall DataFrame in the
    canonical schema. Models / dates that fail are skipped, not raised.

    ``model_names``: subset of ``registry.REGISTRY`` keys. ``None`` runs
    every registered model."""
    selected = list(model_names or REGISTRY.keys())
    missing = [m for m in selected if m not in REGISTRY]
    if missing:
        raise KeyError(f"Unknown model(s): {missing}. Known: {sorted(REGISTRY)}")

    all_rows: list[dict] = []
    for model in selected:
        entry = REGISTRY[model]
        logger.info("=== %s : replaying %d dates ===", model, len(target_dates))
        for d in target_dates:
            all_rows.extend(replay_one(entry, d, hub=hub, cache_dir=cache_dir))

    if not all_rows:
        df = pd.DataFrame({c: pd.Series(dtype="object") for c in ALL_COLUMNS})
    else:
        df = pd.DataFrame(all_rows, columns=ALL_COLUMNS)
    assert_schema(df)
    return df.sort_values(["target_date", "hour_ending", "model_name"]).reset_index(
        drop=True
    )
