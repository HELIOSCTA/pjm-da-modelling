"""Generate all input data validation HTML reports.

Produces one HTML report per fragment group, plus a master dashboard that
iframes them all together. Output goes to ``modelling/html_reports/output/``.

Usage:
    python modelling/html_reports/generate_report.py
    python modelling/html_reports/generate_report.py --only fuel_mix
    python modelling/html_reports/generate_report.py --upload
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

# Ensure ``modelling/`` is importable regardless of CWD (matches validate_cache.py).
_MODELLING_ROOT = Path(__file__).resolve().parent.parent
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

from da_models.common import configs  # noqa: E402
from html_reports.fragments import fuel_mix as fuel_mix_fragments  # noqa: E402
from html_reports.fragments import load_forecast as load_forecast_fragments  # noqa: E402
from html_reports.fragments import meteologica_rto_forecast_snapshot as meteo_snapshot_fragments  # noqa: E402
from html_reports.fragments import outages as outages_fragments  # noqa: E402
from html_reports.fragments import pjm_rto_forecast_snapshot as pjm_snapshot_fragments  # noqa: E402
from html_reports.fragments import solar_forecast as solar_forecast_fragments  # noqa: E402
from html_reports.fragments import wind_forecast as wind_forecast_fragments  # noqa: E402
from html_reports.utils.html_dashboard import HTMLDashboardBuilder  # noqa: E402
from html_reports.utils.master_report import build_master  # noqa: E402
from utils.logging_utils import init_logging  # noqa: E402

BLOB_PREFIX = "pjm-da/reports"

REPORT_OUTPUT_DIR = Path(__file__).parent / "output"

FRAGMENT_REGISTRY = {
    # ────── FORECASTS ────────────────────────────────────────────
    "load_forecast":              ("Load Forecast",                load_forecast_fragments.build_fragments),
    "solar_forecast":             ("Solar Forecast",               solar_forecast_fragments.build_fragments),
    "wind_forecast":              ("Wind Forecast",                wind_forecast_fragments.build_fragments),
    "pjm_rto_forecast_snapshot":    ("PJM RTO Forecast Snapshot",            pjm_snapshot_fragments.build_fragments),
    "pjm_west_forecast_snapshot":   ("PJM Western Forecast Snapshot",        pjm_snapshot_fragments.build_fragments_west),
    "pjm_midatl_forecast_snapshot": ("PJM Mid-Atlantic Forecast Snapshot",   pjm_snapshot_fragments.build_fragments_midatl),
    "pjm_south_forecast_snapshot":  ("PJM Southern Forecast Snapshot",       pjm_snapshot_fragments.build_fragments_south),
    "meteologica_rto_forecast_snapshot":    ("Meteologica RTO Forecast Snapshot",          meteo_snapshot_fragments.build_fragments),
    "meteologica_west_forecast_snapshot":   ("Meteologica Western Forecast Snapshot",      meteo_snapshot_fragments.build_fragments_west),
    "meteologica_midatl_forecast_snapshot": ("Meteologica Mid-Atlantic Forecast Snapshot", meteo_snapshot_fragments.build_fragments_midatl),
    "meteologica_south_forecast_snapshot":  ("Meteologica Southern Forecast Snapshot",     meteo_snapshot_fragments.build_fragments_south),
    "fuel_mix":                   ("Fuel Mix",                     fuel_mix_fragments.build_fragments),

    # ────── OUTAGES ──────────────────────────────────────────────
    "outages":                    ("Outages",                      outages_fragments.build_fragments),
}


def generate(
    output_dir: Path | None = None,
    cache_dir: Path | None = None,
    upload: bool = False,
    only: list[str] | None = None,
    pl=None,
) -> dict[str, Path]:
    """Generate individual + master validation reports."""
    output_dir = output_dir or REPORT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = cache_dir or configs.CACHE_DIR

    if pl:
        pl.header("DA Model Reports")
        pl.section("Cache Configuration")
        pl.info(f"Cache dir: {cache_dir}")

    cache_kwargs = dict(
        cache_dir=cache_dir,
        cache_enabled=True,
        cache_ttl_hours=24.0,
        force_refresh=False,
    )

    results: dict[str, Path] = {}

    if pl:
        pl.section("Building Reports")

    registry = {k: v for k, v in FRAGMENT_REGISTRY.items() if k in only} if only else FRAGMENT_REGISTRY

    for source_key, (label, build_fn) in registry.items():
        if pl:
            with pl.timer(f"{label} report"):
                fragments = build_fn(schema=None, **cache_kwargs)
        else:
            fragments = build_fn(schema=None, **cache_kwargs)

        builder = HTMLDashboardBuilder(
            title=f"{label} Validation — {date.today().isoformat()}",
            theme="dark",
        )
        _feed_fragments(builder, fragments)

        filename = f"validation_{source_key}_{date.today().isoformat()}.html"
        output_path = output_dir / filename
        builder.save(str(output_path))

        if pl:
            pl.success(f"Saved: {output_path}")
        results[source_key] = output_path

    if len(results) >= 1:
        if pl:
            pl.section("Building Master Report")
        master_path = build_master(results, output_dir)
        if pl:
            pl.success(f"Saved: {master_path}")
        results["master"] = master_path

    if upload:
        _upload_to_blob(results, pl=pl)

    if pl:
        pl.section("Summary")
        for source, path in results.items():
            if source == "blob_urls":
                continue
            pl.info(f"  {source}: {path}")
        if "blob_urls" in results:
            for source, url in results["blob_urls"].items():
                pl.info(f"  {source} (blob): {url}")
        pl.success(f"{len(results)} reports generated")

    return results


def _upload_to_blob(results: dict[str, Path], pl=None) -> None:
    """Best-effort upload to Azure Blob Storage.

    Gated lazily — if the blob client isn't available in this repo layout,
    warn and skip rather than breaking the report generation path.
    """
    import os

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        msg = "AZURE_STORAGE_CONNECTION_STRING not set — skipping upload"
        if pl:
            pl.warning(msg)
        return

    try:
        from utils.azure_blob_storage_utils import AzureBlobStorageClient  # type: ignore
    except ImportError:
        msg = "Azure blob storage client not available in modelling/utils — skipping upload"
        if pl:
            pl.warning(msg)
        return

    if pl:
        pl.section("Uploading to Azure Blob Storage")

    blob_client = AzureBlobStorageClient()
    blob_prefix = f"{BLOB_PREFIX}/{date.today().isoformat()}"
    blob_urls: dict[str, str] = {}

    for key, path in results.items():
        if key == "master":
            continue
        blob_name = f"{blob_prefix}/{path.name}"
        url = blob_client.upload_file(
            file_path=path,
            blob_name=blob_name,
            content_type="text/html",
        )
        blob_urls[key] = url
        if pl:
            pl.info(f"Uploaded {key}: {url}")

    if "master" in results:
        master_html = results["master"].read_text(encoding="utf-8")
        for key, url in blob_urls.items():
            local_filename = results[key].name
            master_html = master_html.replace(f'src="{local_filename}"', f'src="{url}"')
            master_html = master_html.replace(f'value="{local_filename}"', f'value="{url}"')

        master_blob_name = f"{blob_prefix}/master_report.html"
        master_url = blob_client.upload_html(
            html_content=master_html,
            blob_name=master_blob_name,
        )
        blob_urls["master"] = master_url
        if pl:
            pl.info(f"Uploaded master: {master_url}")

    results["blob_urls"] = blob_urls
    if pl:
        pl.success(f"Uploaded {len(blob_urls)} reports to Azure Blob Storage")


def _feed_fragments(builder: HTMLDashboardBuilder, fragments: list) -> None:
    """Feed a fragment list (dividers + sections) into a builder."""
    for item in fragments:
        if isinstance(item, str):
            builder.add_divider(item)
        else:
            name, content, icon = item
            builder.add_content(name, content, icon=icon)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate DA model validation reports")
    parser.add_argument(
        "--upload", action="store_true",
        help="Upload reports to Azure Blob Storage after generation",
    )
    parser.add_argument(
        "--only", nargs="+", choices=list(FRAGMENT_REGISTRY.keys()), metavar="KEY",
        help=f"Generate only specified report sections: {', '.join(FRAGMENT_REGISTRY.keys())}",
    )
    return parser.parse_args()


def main():
    args = _parse_args()

    pl = init_logging(
        name="generate_report",
        log_dir=_MODELLING_ROOT / "logs",
    )
    try:
        generate(
            upload=args.upload,
            only=args.only,
            pl=pl,
        )
    finally:
        pl.close()


if __name__ == "__main__":
    main()
