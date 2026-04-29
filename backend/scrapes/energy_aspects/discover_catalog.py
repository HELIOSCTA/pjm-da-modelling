"""
Energy Aspects API - discover available data sources and endpoints.

On-demand utility that catalogs:
  1. Known API endpoints.
  2. Available timeseries datasets from /datasets/timeseries.
  3. Valid metadata filter values from /metadata/timeseries.
  4. Website-to-API dataset mappings from /dataset_mappings.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if sys.path[0] != str(PROJECT_ROOT):
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.scrapes.energy_aspects import energy_aspects_api_utils as ea_api
from backend.utils import logging_utils

API_SCRAPE_NAME = "energy_aspects_discover_catalog"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=False,
)

OUTPUT_DIR = Path(__file__).parent / "output"

ENDPOINT_CATALOG = [
    {
        "category": "Datasets",
        "endpoint": "List timeseries datasets",
        "method": "GET",
        "path": "/datasets/timeseries",
        "description": "List all available timeseries datasets.",
        "parameters": "api_key, page, records_per_page",
        "notes": "Returns dataset_id, name, and metadata fields for each dataset.",
    },
    {
        "category": "Datasets",
        "endpoint": "Get dataset detail",
        "method": "GET",
        "path": "/datasets/timeseries/{dataset_id}",
        "description": "Get detailed info about a specific timeseries dataset.",
        "parameters": "api_key, dataset_id",
        "notes": "Use dataset_id values from the list endpoint.",
    },
    {
        "category": "Dataset Mappings",
        "endpoint": "Dataset mappings",
        "method": "GET",
        "path": "/dataset_mappings",
        "description": "Maps Energy Aspects website files to API dataset IDs.",
        "parameters": "api_key",
        "notes": "Useful for translating known website downloads to API dataset IDs.",
    },
    {
        "category": "Timeseries",
        "endpoint": "Timeseries data",
        "method": "GET",
        "path": "/timeseries",
        "description": "Retrieve timeseries data in JSON format.",
        "parameters": "api_key, dataset_id, date_from, date_to, release_date, metadata filters",
        "notes": "Dates can be YYYY-MM-DD or dynamic notation.",
    },
    {
        "category": "Timeseries",
        "endpoint": "Timeseries data CSV",
        "method": "GET",
        "path": "/timeseries/csv",
        "description": "Retrieve timeseries data in CSV format.",
        "parameters": "Same as /timeseries, plus column_header",
        "notes": "Used by the timeseries scrapes in this package.",
    },
    {
        "category": "Timeseries",
        "endpoint": "Timeseries data XLSX",
        "method": "GET",
        "path": "/timeseries/xlsx",
        "description": "Retrieve timeseries data in Excel format.",
        "parameters": "Same as /timeseries, plus column_header",
        "notes": "Append /xlsx for Excel output.",
    },
    {
        "category": "Timeseries",
        "endpoint": "Timeseries data paginated",
        "method": "GET",
        "path": "/timeseries/pagination/",
        "description": "Paginated timeseries retrieval.",
        "parameters": "Same as /timeseries, plus page and records_per_page",
        "notes": "Use pagination for large result sets.",
    },
    {
        "category": "Changelog",
        "endpoint": "Changelog",
        "method": "GET",
        "path": "/changelog",
        "description": "Dataset changelog showing update times.",
        "parameters": "api_key, date_from, date_to, dataset_id, page, records_per_page",
        "notes": "date_from and date_to are required.",
    },
    {
        "category": "Metadata",
        "endpoint": "Timeseries metadata",
        "method": "GET",
        "path": "/metadata/timeseries",
        "description": "Returns valid parameter values for timeseries filtering.",
        "parameters": "api_key",
        "notes": "Call before querying /timeseries with metadata filters.",
    },
]


def _pull_metadata() -> dict:
    logger.section("Fetching metadata/timeseries...")
    data = ea_api.get_json("/metadata/timeseries")
    logger.info(f"Metadata response type: {type(data).__name__}")
    return data


def _pull_datasets() -> list[dict]:
    logger.section("Fetching datasets/timeseries...")
    datasets = ea_api.get_paginated("/datasets/timeseries", records_per_page=5000)
    logger.info(f"Found {len(datasets)} datasets")
    return datasets


def _pull_dataset_mappings() -> dict | list:
    logger.section("Fetching dataset_mappings...")
    data = ea_api.get_json("/dataset_mappings")
    count = len(data) if isinstance(data, list) else "N/A"
    logger.info(f"Mappings response type: {type(data).__name__}, count: {count}")
    return data


def _format_catalog(
    metadata: dict,
    datasets: list[dict],
    dataset_mappings: dict | list,
) -> dict:
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "api_base_url": ea_api.BASE_URL,
        "auth_method": "api_key query parameter",
        "static_endpoints": ENDPOINT_CATALOG,
        "dynamic": {
            "metadata_filter_values": metadata,
            "datasets": datasets,
            "dataset_count": len(datasets),
            "dataset_mappings": dataset_mappings,
        },
    }


def _save_catalog(catalog: dict) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "catalog.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, default=str)
    return output_path


def main() -> dict:
    try:
        logger.header(API_SCRAPE_NAME)

        metadata = _pull_metadata()
        datasets = _pull_datasets()
        dataset_mappings = _pull_dataset_mappings()

        logger.section("Assembling catalog...")
        catalog = _format_catalog(metadata, datasets, dataset_mappings)

        output_path = _save_catalog(catalog)
        logger.success(f"Catalog written to {output_path}")

        logger.section("Summary")
        logger.info(f"Static endpoints documented: {len(ENDPOINT_CATALOG)}")
        logger.info(f"Datasets discovered: {catalog['dynamic']['dataset_count']}")

        if datasets:
            logger.info("First 10 datasets:")
            for dataset in datasets[:10]:
                dataset_id = dataset.get("dataset_id", "?")
                metadata = dataset.get("metadata", {})
                description = metadata.get("description", "?")
                category = metadata.get("category", "")
                frequency = metadata.get("frequency", "")
                logger.info(f"  - [{dataset_id}] {description} ({category}, {frequency})")
            if len(datasets) > 10:
                logger.info(f"  ... and {len(datasets) - 10} more")

        return catalog

    except Exception as e:
        logger.exception(f"Catalog discovery failed: {e}")
        raise

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    catalog = main()
