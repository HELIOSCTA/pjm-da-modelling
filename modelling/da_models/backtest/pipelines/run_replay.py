"""Replay every (model, date) cell over a window, write one parquet.

v1 default window = last 7 *settled* delivery dates (D-7..D-1 EPT).
Edit the module-level constants below or pass overrides to ``run(...)``
from a REPL / notebook. Per the python-scripts skill, the artefacts
land under ``backtest/output/`` with a ``{run_id}.parquet`` +
``{run_id}_meta.json`` pair.

Usage::

    python -m da_models.backtest.pipelines.run_replay
    python modelling/da_models/backtest/pipelines/run_replay.py
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[3]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))


from da_models.backtest import configs as C  # noqa: E402
from da_models.backtest.registry import REGISTRY  # noqa: E402
from da_models.backtest.replay import replay_grid  # noqa: E402
from utils.logging_utils import init_logging, print_divider, print_header  # noqa: E402

# -- Defaults (edit here instead of using CLI flags) -----------------------
TARGET_DATES: list[date] | None = None  # None -> configs.default_target_dates()
MODEL_NAMES: tuple[str, ...] | None = None  # None -> configs.DEFAULT_MODEL_NAMES
HUB: str = C.HUB
CACHE_DIR: Path | None = None
OUTPUT_DIR: Path = C.OUTPUT_DIR
LOG_DIR: Path = _MODELLING_ROOT / "logs"


def _run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    return f"{stamp}_{uuid.uuid4().hex[:6]}"


def run(
    target_dates: list[date] | None = TARGET_DATES,
    model_names: tuple[str, ...] | None = MODEL_NAMES,
    hub: str = HUB,
    cache_dir: Path | None = CACHE_DIR,
    output_dir: Path = OUTPUT_DIR,
    quiet: bool = False,
) -> dict:
    """Run the replay and persist a parquet + meta-json. Returns a dict:
    ``run_id``, ``output_parquet``, ``output_meta``, ``frame`` (the tall
    DataFrame), ``target_dates``, ``models``, ``rows``."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="backtest_replay", log_dir=LOG_DIR)
    try:
        dates = (
            list(target_dates) if target_dates is not None else C.default_target_dates()
        )
        models = (
            list(model_names)
            if model_names is not None
            else [m for m in C.DEFAULT_MODEL_NAMES if m in REGISTRY]
        )
        rid = _run_id()
        output_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = output_dir / f"{rid}.parquet"
        meta_path = output_dir / f"{rid}_meta.json"

        if not quiet:
            print_header(f"BACKTEST REPLAY  |  run_id {rid}", "=", 100)
            print(f"  Hub            {hub}")
            print(
                f"  Date range     {dates[0]} -> {dates[-1]}  ({len(dates)} delivery days)"
            )
            print(f"  Models         {', '.join(models)}")
            print(f"  Cells          {len(models) * len(dates)} (model x date)")
            print(f"  Output         {parquet_path}")
            print()

        with pl.timer(f"replay {len(models)} models x {len(dates)} dates"):
            df = replay_grid(dates, models, hub=hub, cache_dir=cache_dir)

        df.to_parquet(parquet_path, index=False)
        meta = {
            "run_id": rid,
            "kind": "backtest_replay",
            "hub": hub,
            "target_dates": [d.isoformat() for d in dates],
            "models": models,
            "rows": int(len(df)),
            "registry_known_models": sorted(REGISTRY.keys()),
        }
        meta_path.write_text(json.dumps(meta, indent=2))

        if not quiet:
            counts = df.groupby("model_name").size().to_dict()
            actuals = (
                df.groupby("model_name")["actual_lmp"]
                .apply(lambda s: int(s.notna().sum()))
                .to_dict()
            )
            print(f"  Rows written   {len(df):,}")
            for m in models:
                print(
                    f"    {m:<28} rows={counts.get(m, 0):>4}   with-actual={actuals.get(m, 0):>4}"
                )
            print()
            print_divider("=", 100, dim=False)
            print()

        return {
            "run_id": rid,
            "output_parquet": parquet_path,
            "output_meta": meta_path,
            "frame": df,
            "target_dates": dates,
            "models": models,
            "rows": int(len(df)),
        }
    finally:
        pl.close()


if __name__ == "__main__":
    run()
