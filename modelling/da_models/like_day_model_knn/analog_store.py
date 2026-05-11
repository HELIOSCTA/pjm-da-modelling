"""Parquet explainability store for like_day_model_knn backtests.

Post-T4 the pool is long-format (one row per (date, hour_ending), scalar
feature cols), and ``find_twins`` returns a per-HE analog table with
columns ``hour_ending``, ``rank``, ``date``, ``distance``, ``weight``,
``lmp``. The store now writes two parquets per run:

  - ``runs/<run_id>.parquet``           — single-row manifest with the
    config + spec snapshot and pool/candidate counts.
  - ``analog_picks/<run_id>.parquet``   — the analogs DataFrame as-is,
    tagged with ``run_id`` and ``target_date``. One row per (HE, rank)
    selected analog with its distance, blend weight, and analog LMP.

The pre-T4 wide-format helpers (``_explain_day_candidates``,
``_explain_hour_candidates``, ``_build_*_picks``, ``_build_*_contributions``,
``_build_feature_price_correlations``, ``_window_columns``, etc.) were
unreachable under the long-pool engine and were deleted in T4 Session 3.
If richer explainability is needed, rebuild it directly off the long-
format pool/query/analogs frames — those carry the same information
the wide-format frames did, and per-HE row filters are simpler than
the windowed-column slicing the old helpers performed.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from da_models.like_day_model_knn.configs import KnnModelConfig, ModelSpec

DEFAULT_STORE_DIR = Path(__file__).resolve().parent / "output" / "analog_store"


def write_analog_explainability(
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    pool: pd.DataFrame,
    query: pd.DataFrame,
    analogs: pd.DataFrame,
    output_dir: Path | None = None,
    run_id: str | None = None,
) -> str:
    """Append a single run's analog tables to the parquet store.

    ``pool`` and ``query`` must be long-format (carry an ``hour_ending``
    column). ``analogs`` is the per-HE pick table from ``find_twins``.
    The ``query`` arg is accepted for API stability with the pre-T4
    signature but isn't currently persisted — the spec snapshot in the
    run manifest is enough to reproduce it.

    ``run_id`` lets the caller thread the same identifier into other
    artefacts (e.g. the blob forecast publisher). Default ``None``
    generates a fresh UUID, preserving prior behaviour.
    """
    del query  # reserved for API stability; not persisted yet

    if run_id is None:
        run_id = str(uuid.uuid4())
    output_dir = Path(output_dir) if output_dir is not None else DEFAULT_STORE_DIR
    _ensure_store_dirs(output_dir)

    if pool is None or "hour_ending" not in pool.columns:
        raise ValueError(
            "write_analog_explainability requires a long-format pool"
            " (row per (date, hour_ending)). Wide-format pools are no"
            " longer supported as of T4."
        )

    n_pool_rows = int(len(pool))
    n_pool_dates = int(pool["date"].nunique())
    n_analog_rows = int(len(analogs))

    _write_run_manifest(
        output_dir=output_dir,
        run_id=run_id,
        target_date=target_date,
        config=config,
        spec=spec,
        n_pool_rows=n_pool_rows,
        n_pool_dates=n_pool_dates,
        n_analog_rows=n_analog_rows,
    )

    if n_analog_rows == 0:
        return run_id

    picks = analogs.copy()
    picks.insert(0, "run_id", run_id)
    picks.insert(1, "target_date", str(target_date))
    _write_table(output_dir / "analog_picks" / f"{run_id}.parquet", picks)
    return run_id


# ── helpers ───────────────────────────────────────────────────────────────


def _ensure_store_dirs(output_dir: Path) -> None:
    for name in ("runs", "analog_picks"):
        (output_dir / name).mkdir(parents=True, exist_ok=True)


def _write_run_manifest(
    output_dir: Path,
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    n_pool_rows: int,
    n_pool_dates: int,
    n_analog_rows: int,
) -> None:
    row = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "created_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
                "target_date": str(target_date),
                "model_name": spec.name,
                "match_unit": spec.match_unit,
                "description": spec.description,
                "hub": config.hub,
                "schema": config.schema,
                "n_analogs": int(config.n_analogs),
                "season_window_days": int(config.season_window_days),
                "min_pool_size": int(config.min_pool_size),
                "n_pool_rows": int(n_pool_rows),
                "n_pool_dates": int(n_pool_dates),
                "n_analog_rows": int(n_analog_rows),
            }
        ]
    )
    _write_table(output_dir / "runs" / f"{run_id}.parquet", row)


def _write_table(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
