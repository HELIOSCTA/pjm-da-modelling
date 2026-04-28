"""Generate an HTML report of target-vs-analogs for forward_only_knn.

Walks the named configs in ``experiments.registry.CONFIG_REGISTRY``, runs each
through ``run_forecast`` for one target date, and writes a single dashboard to
``modelling/html_reports/output/forward_only_knn_analogs_{date}.html``.

Usage:
    python -m html_reports.forward_only_knn.generate_report --date 2026-04-27
    python -m html_reports.forward_only_knn.generate_report --date 2026-04-27 \
        --configs baseline tight_system
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

# Ensure ``modelling/`` is importable regardless of CWD.
_MODELLING_ROOT = Path(__file__).resolve().parents[2]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

from da_models.forward_only_knn.experiments.registry import CONFIG_REGISTRY  # noqa: E402
from html_reports.forward_only_knn.fragments import analogs as analog_fragments  # noqa: E402
from html_reports.utils.html_dashboard import HTMLDashboardBuilder  # noqa: E402
from utils.logging_utils import init_logging  # noqa: E402

REPORT_OUTPUT_DIR = _MODELLING_ROOT / "html_reports" / "output"


def generate(
    target_date: date,
    config_names: list[str] | None = None,
    output_dir: Path | None = None,
    top_n: int = analog_fragments.DEFAULT_TOP_N,
    pl=None,
) -> Path:
    output_dir = output_dir or REPORT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    selected = list(config_names) if config_names else list(CONFIG_REGISTRY.keys())
    unknown = [n for n in selected if n not in CONFIG_REGISTRY]
    if unknown:
        raise SystemExit(f"Unknown configs: {unknown}. Known: {sorted(CONFIG_REGISTRY)}")

    if pl:
        pl.header(f"forward_only_knn analog report — {target_date}")
        pl.section("Configs")
        pl.info(f"Running: {', '.join(selected)}")

    builder = HTMLDashboardBuilder(
        title=f"forward_only_knn — analogs for {target_date}",
        theme="dark",
    )

    for name in selected:
        cfg = CONFIG_REGISTRY[name]()
        if pl:
            with pl.timer(f"{name}"):
                sections = analog_fragments.build_fragments(target_date, name, cfg, top_n=top_n)
        else:
            sections = analog_fragments.build_fragments(target_date, name, cfg, top_n=top_n)

        builder.add_divider(name)
        for label, content, icon in sections:
            builder.add_content(label, content, icon=icon)

    filename = f"forward_only_knn_analogs_{target_date}.html"
    output_path = output_dir / filename
    builder.save(str(output_path))
    if pl:
        pl.success(f"Saved: {output_path}")
    return output_path


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate forward_only_knn target-vs-analogs HTML report",
    )
    parser.add_argument(
        "--date", type=_parse_date,
        help="Target date YYYY-MM-DD (defaults to today).",
    )
    parser.add_argument(
        "--configs", nargs="*",
        help="Subset of registered config names. Default: all.",
    )
    parser.add_argument(
        "--top-n", type=int, default=analog_fragments.DEFAULT_TOP_N,
        help=f"Number of analogs to overlay per config (default: {analog_fragments.DEFAULT_TOP_N}).",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List registered config names and exit.",
    )
    return parser.parse_args()


def main() -> None:
    # Force UTF-8 on Windows so logger output doesn't blow up on cp1252.
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    args = _parse_args()

    if args.list:
        for name in CONFIG_REGISTRY:
            print(name)
        return

    pl = init_logging(
        name="forward_only_knn_report",
        log_dir=_MODELLING_ROOT / "logs",
    )
    try:
        target = args.date or date.today()
        generate(
            target_date=target,
            config_names=args.configs,
            top_n=args.top_n,
            pl=pl,
        )
    finally:
        pl.close()


if __name__ == "__main__":
    main()
