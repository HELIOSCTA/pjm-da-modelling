"""Daily named-config scoreboard for forward-only KNN.

Runs every config in the registry against a single target date, extracts the
hourly Error row from the forecast pipeline, and appends one row per config
to a long-format CSV. Use --summary to inspect rolling performance across
the accumulated history.

Usage:
    # Daily run (after DA cleared and actuals are in)
    python -m da_models.forward_only_knn.experiments.evaluate_configs --date 2026-04-28

    # Inspect the accumulated scoreboard
    python -m da_models.forward_only_knn.experiments.evaluate_configs --summary

    # Limit to a subset of configs
    python -m da_models.forward_only_knn.experiments.evaluate_configs --date 2026-04-28 \
        --configs baseline tight_system
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

# Ensure modelling/ is importable when invoked as a script.
_MODELLING_ROOT = Path(__file__).resolve().parents[3]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

from da_models.forward_only_knn import configs as fwd_configs  # noqa: E402
from da_models.forward_only_knn.configs import ForwardOnlyKNNConfig  # noqa: E402
from da_models.forward_only_knn.experiments.registry import CONFIG_REGISTRY  # noqa: E402
from da_models.forward_only_knn.pipelines.forecast import run_forecast  # noqa: E402
from utils.logging_utils import (  # noqa: E402
    init_logging,
    print_divider,
    print_header,
    print_section,
)

logger = logging.getLogger(__name__)

SCOREBOARD_PATH = _MODELLING_ROOT / "logs" / "forward_only_knn_config_scoreboard.csv"

HOURS = list(range(1, 25))
DEFAULT_BASELINE = "baseline"

SCOREBOARD_COLUMNS = [
    "run_timestamp",
    "target_date",
    "config_name",
    "config_hash",
    "config_json",
    "hub",
    "n_analogs",
    "has_actuals",
    "actual_onpeak",
    "actual_offpeak",
    "actual_flat",
    "forecast_onpeak",
    "forecast_offpeak",
    "forecast_flat",
    "onpeak_error",
    "offpeak_error",
    "flat_error",
    "onpeak_mae",
    "offpeak_mae",
    "flat_mae",
    "max_hourly_abs_error",
    "worst_hour",
    "actual_hourly_json",
    "forecast_hourly_json",
    "hourly_errors_json",
]


def _config_fingerprint(cfg: ForwardOnlyKNNConfig) -> tuple[str, str]:
    """Return (short_hash, canonical_json) of distance-affecting fields."""
    weights = cfg.feature_group_weights or fwd_configs.FEATURE_GROUP_WEIGHTS
    payload = {
        "feature_group_weights": dict(weights),
        "n_analogs": cfg.n_analogs,
        "season_window_days": cfg.season_window_days,
        "recency_half_life_days": cfg.recency_half_life_days,
        "same_dow_group": cfg.same_dow_group,
        "exclude_holidays": cfg.exclude_holidays,
        "min_pool_size": cfg.min_pool_size,
        "weight_method": cfg.weight_method,
        "hub": cfg.hub,
        "schema": cfg.schema,
        "quantiles": cfg.resolved_quantiles(),
    }
    canonical = json.dumps(payload, sort_keys=True)
    return hashlib.sha1(canonical.encode()).hexdigest()[:12], canonical


def _score_one(
    name: str,
    cfg: ForwardOnlyKNNConfig,
    target_date: date,
) -> dict | None:
    """Run a single config and return one row dict, or None if no scorable result."""
    with open(os.devnull, "w", encoding="utf-8") as sink, contextlib.redirect_stdout(sink):
        try:
            result = run_forecast(target_date=target_date, config=cfg)
        except Exception as exc:
            logger.exception("Forecast crashed for config=%s date=%s: %s", name, target_date, exc)
            return None

    if "error" in result:
        logger.error("Forecast failed for %s: %s", name, result["error"])
        return None

    if not result.get("has_actuals"):
        logger.warning(
            "No actuals for %s (config=%s). Run log: %s",
            target_date, name, log_path,
        )
        return None

    out = result["output_table"]
    try:
        err = out[out["Type"] == "Error"].iloc[0]
        act = out[out["Type"] == "Actual"].iloc[0]
        fcst = out[out["Type"] == "Forecast"].iloc[0]
    except (IndexError, KeyError):
        logger.warning("Output table missing required rows for %s (config=%s)", target_date, name)
        return None

    analogs_df = result.get("analogs")
    analogs_top: list[dict] = []
    if analogs_df is not None and len(analogs_df) > 0:
        cols = [c for c in ("rank", "date", "distance", "weight") if c in analogs_df.columns]
        top = analogs_df.head(10)[cols].copy()
        if "date" in top.columns:
            top["date"] = pd.to_datetime(top["date"]).dt.date.astype(str)
        analogs_top = top.to_dict("records")

    def _hourly(row) -> dict[int, float | None]:
        return {h: (None if pd.isna(row[f"HE{h}"]) else float(row[f"HE{h}"])) for h in HOURS}

    err_hourly = _hourly(err)
    act_hourly = _hourly(act)
    fcst_hourly = _hourly(fcst)

    abs_hourly = {h: abs(v) for h, v in err_hourly.items() if v is not None}
    if abs_hourly:
        worst_hour = max(abs_hourly, key=abs_hourly.get)
        max_abs = float(abs_hourly[worst_hour])
    else:
        worst_hour = None
        max_abs = float("nan")

    short_hash, canonical = _config_fingerprint(cfg)
    return {
        "run_timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "target_date": str(target_date),
        "config_name": name,
        "config_hash": short_hash,
        "config_json": canonical,
        "hub": cfg.hub,
        "n_analogs": cfg.n_analogs,
        "has_actuals": True,
        "actual_onpeak": float(act["OnPeak"]),
        "actual_offpeak": float(act["OffPeak"]),
        "actual_flat": float(act["Flat"]),
        "forecast_onpeak": float(fcst["OnPeak"]),
        "forecast_offpeak": float(fcst["OffPeak"]),
        "forecast_flat": float(fcst["Flat"]),
        "onpeak_error": float(err["OnPeak"]),
        "offpeak_error": float(err["OffPeak"]),
        "flat_error": float(err["Flat"]),
        "onpeak_mae": abs(float(err["OnPeak"])),
        "offpeak_mae": abs(float(err["OffPeak"])),
        "flat_mae": abs(float(err["Flat"])),
        "max_hourly_abs_error": max_abs,
        "worst_hour": int(worst_hour) if worst_hour is not None else None,
        "actual_hourly_json": json.dumps({str(h): v for h, v in act_hourly.items()}),
        "forecast_hourly_json": json.dumps({str(h): v for h, v in fcst_hourly.items()}),
        "hourly_errors_json": json.dumps({str(h): v for h, v in err_hourly.items()}),
        # Not persisted to CSV — used for the per-config preview block.
        "analogs_top10": analogs_top,
    }


def _append_rows(rows: list[dict]) -> None:
    if not rows:
        return
    SCOREBOARD_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=SCOREBOARD_COLUMNS)
    write_header = not SCOREBOARD_PATH.exists()
    df.to_csv(SCOREBOARD_PATH, mode="a", header=write_header, index=False)
    logger.info("Appended %d rows to %s", len(df), SCOREBOARD_PATH)


def _fmt_cell(value: float | None, width: int = 6, prec: int = 1) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return f"{'':>{width}}"
    return f"{value:>{width}.{prec}f}"


def _print_analogs_block(analogs: list[dict]) -> None:
    print_section("Top analog days")
    if not analogs:
        print("  (no analogs returned)")
        return
    print(f"  {'rank':<5} {'date':<22} {'distance':>10} {'weight':>10}")
    print_divider("-", length=52)
    for a in analogs:
        try:
            d = pd.to_datetime(a["date"]).date()
            label = d.strftime("%a %b-%d %Y")
        except Exception:
            label = str(a.get("date", "?"))
        print(
            f"  {int(a.get('rank', 0)):<5} {label:<22} "
            f"{float(a.get('distance', float('nan'))):>10.4f} "
            f"{float(a.get('weight', float('nan'))):>10.4f}"
        )


def _print_hourly_block(row: dict) -> None:
    print_section("Hourly forecast vs actual ($/MWh)")
    actual = json.loads(row["actual_hourly_json"])
    forecast = json.loads(row["forecast_hourly_json"])
    error = json.loads(row["hourly_errors_json"])

    header = f"  {'Type':<10}"
    for h in HOURS:
        header += f" {('HE'+str(h)):>6}"
    header += f" {'OnPk':>8} {'OffPk':>8} {'Flat':>8}"
    print(header)
    print_divider("-", length=len(header))

    series = [
        ("Actual",   actual,   row["actual_onpeak"],   row["actual_offpeak"],   row["actual_flat"]),
        ("Forecast", forecast, row["forecast_onpeak"], row["forecast_offpeak"], row["forecast_flat"]),
        ("Error",    error,    row["onpeak_error"],    row["offpeak_error"],    row["flat_error"]),
    ]
    for label, hourly, onpk, offpk, flat in series:
        line = f"  {label:<10}"
        for h in HOURS:
            line += f" {_fmt_cell(hourly.get(str(h)))}"
        line += f" {_fmt_cell(onpk, width=8, prec=2)}"
        line += f" {_fmt_cell(offpk, width=8, prec=2)}"
        line += f" {_fmt_cell(flat, width=8, prec=2)}"
        print(line)


def _print_config_block(row: dict) -> None:
    title = (
        f"{row['config_name']}  |  "
        f"flat_mae={row['flat_mae']:.2f}  flat_err={row['flat_error']:+.2f}  "
        f"hash={row['config_hash']}"
    )
    print_header(title, length=120)
    _print_analogs_block(row.get("analogs_top10", []))
    _print_hourly_block(row)


def _print_today_preview(rows: list[dict], target_date: date) -> None:
    if not rows:
        print(f"\nNo scorable rows for {target_date}.")
        return

    print_header(
        f"Scoreboard for {target_date}  ({rows[0]['hub']})",
        length=120,
    )

    for row in sorted(rows, key=lambda r: r["flat_mae"]):
        _print_config_block(row)


def run_scoreboard(target_date: date, names: Iterable[str] | None = None) -> list[dict]:
    selected = list(names) if names else list(CONFIG_REGISTRY.keys())
    unknown = [n for n in selected if n not in CONFIG_REGISTRY]
    if unknown:
        raise SystemExit(
            f"Unknown configs: {unknown}. Known: {sorted(CONFIG_REGISTRY)}"
        )

    rows: list[dict] = []
    for name in selected:
        cfg = CONFIG_REGISTRY[name]()
        logger.info("Running %s for %s ...", name, target_date)
        row = _score_one(name, cfg, target_date)
        if row is not None:
            rows.append(row)

    # CSV scoreboard disabled for now — re-enable by uncommenting:
    # _append_rows(rows)
    _print_today_preview(rows, target_date)
    return rows


def _window_table(df: pd.DataFrame, label: str, baseline_name: str) -> None:
    if len(df) == 0:
        print(f"\n--- {label} (no rows) ---")
        return
    grp = df.groupby("config_name").agg(
        n=("flat_mae", "count"),
        flat_mae=("flat_mae", "mean"),
        flat_bias=("flat_error", "mean"),
        onpeak_mae=("onpeak_mae", "mean"),
        offpeak_mae=("offpeak_mae", "mean"),
    )
    if baseline_name in grp.index:
        base_mae = grp.loc[baseline_name, "flat_mae"]
        grp["vs_baseline"] = grp["flat_mae"] - base_mae
    else:
        grp["vs_baseline"] = float("nan")
    grp = grp.sort_values("flat_mae")

    print(f"\n--- {label}  (n_dates={df['target_date'].nunique()}) ---")
    print(
        f"  {'config':<28} {'n':>4} {'flat_mae':>9} {'vs_base':>9} "
        f"{'flat_bias':>10} {'onpk_mae':>9} {'offpk_mae':>9}"
    )
    print("  " + "-" * 84)
    for name, row in grp.iterrows():
        vs = row["vs_baseline"]
        vs_str = "    -    " if pd.isna(vs) else f"{vs:>+9.3f}"
        print(
            f"  {name:<28} {int(row['n']):>4} {row['flat_mae']:>9.3f} "
            f"{vs_str} {row['flat_bias']:>+10.3f} "
            f"{row['onpeak_mae']:>9.3f} {row['offpeak_mae']:>9.3f}"
        )


def render_summary(baseline_name: str = DEFAULT_BASELINE) -> None:
    if not SCOREBOARD_PATH.exists():
        print(f"No scoreboard found at {SCOREBOARD_PATH}")
        return

    df = pd.read_csv(SCOREBOARD_PATH)
    if len(df) == 0:
        print("Scoreboard is empty.")
        return

    df["target_date"] = pd.to_datetime(df["target_date"]).dt.date
    df = df.sort_values(["target_date", "config_name"]).reset_index(drop=True)

    print(
        f"\n=== Scoreboard summary  |  rows={len(df)}  "
        f"dates={df['target_date'].nunique()}  "
        f"range={df['target_date'].min()}..{df['target_date'].max()} ==="
    )

    max_d = df["target_date"].max()
    last_30 = df[df["target_date"] > max_d - timedelta(days=30)]
    last_7 = df[df["target_date"] > max_d - timedelta(days=7)]

    _window_table(df,      "ALL",      baseline_name)
    _window_table(last_30, "LAST 30",  baseline_name)
    _window_table(last_7,  "LAST 7",   baseline_name)

    # Same-DOW slice using the latest date's day of week
    latest_dow = pd.Timestamp(max_d).day_name()
    df_dow = df[pd.to_datetime(df["target_date"]).dt.day_name() == latest_dow]
    _window_table(df_dow, f"SAME DOW ({latest_dow})", baseline_name)

    # Detect silent config drift — same name, different hash
    drift = (
        df.groupby("config_name")["config_hash"]
          .nunique()
          .loc[lambda s: s > 1]
    )
    if len(drift) > 0:
        print("\n!! Config drift detected — same name, multiple hashes:")
        for name, n_hashes in drift.items():
            hashes = sorted(df[df["config_name"] == name]["config_hash"].unique())
            print(f"   {name}: {n_hashes} distinct hashes  {hashes}")


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main(argv: list[str] | None = None) -> None:
    # Force UTF-8 on Windows so logger em-dashes don't blow up on cp1252.
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Daily named-config scoreboard for forward-only KNN",
    )
    parser.add_argument(
        "--date", type=_parse_date,
        help="Target date YYYY-MM-DD (defaults to today). Ignored with --summary.",
    )
    parser.add_argument(
        "--configs", nargs="*",
        help="Subset of registry names to run. Default: all.",
    )
    parser.add_argument(
        "--summary", action="store_true",
        help="Render rolling performance from the accumulated CSV instead of running.",
    )
    parser.add_argument(
        "--baseline", default=DEFAULT_BASELINE,
        help=f"Config name to use as baseline in delta column (default: {DEFAULT_BASELINE}).",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List registered config names and exit.",
    )
    args = parser.parse_args(argv)

    if args.list:
        for name in CONFIG_REGISTRY:
            print(name)
        return

    init_logging(name="evaluate_configs", log_dir=_MODELLING_ROOT / "logs")

    if args.summary:
        render_summary(baseline_name=args.baseline)
        return

    target = args.date or datetime.now().date() + timedelta(days=1)
    run_scoreboard(target_date=target, names=args.configs)


if __name__ == "__main__":
    main()
