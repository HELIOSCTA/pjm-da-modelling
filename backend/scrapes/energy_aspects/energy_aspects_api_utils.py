"""
Energy Aspects API utilities for timeseries scrapes.

Base URL: https://api.energyaspects.com/data/
Auth: API key passed as the ``api_key`` query parameter.
"""

import hashlib
import logging
import re
from io import StringIO

import pandas as pd
import requests

from backend import credentials

logger = logging.getLogger(__name__)

BASE_URL = "https://api.energyaspects.com/data"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63


def _get_api_key() -> str:
    key = credentials.ENERGY_ASPECTS_API_KEY
    if not key:
        raise ValueError(
            "ENERGY_ASPECTS_API_KEY is not set. Add it to backend/.env or "
            "the process environment."
        )
    return key


def _redact_params(params: dict) -> dict:
    return {key: ("***" if key == "api_key" else value) for key, value in params.items()}


def get(endpoint: str, params: dict | None = None, timeout: int = 60) -> requests.Response:
    url = f"{BASE_URL}{endpoint}"
    request_params = {"api_key": _get_api_key()}
    if params:
        request_params.update(params)

    logger.debug("GET %s params=%s", url, _redact_params(request_params))
    response = requests.get(url, params=request_params, timeout=timeout)
    response.raise_for_status()
    return response


def get_json(endpoint: str, params: dict | None = None, timeout: int = 60) -> dict | list:
    return get(endpoint, params=params, timeout=timeout).json()


def get_paginated(
    endpoint: str,
    params: dict | None = None,
    records_per_page: int = 5000,
    max_pages: int = 100,
    timeout: int = 60,
) -> list[dict]:
    all_records = []
    page = 1
    request_params = {"records_per_page": records_per_page}
    if params:
        request_params.update(params)

    while page <= max_pages:
        request_params["page"] = page
        data = get_json(endpoint, params=request_params, timeout=timeout)

        if isinstance(data, list):
            if not data:
                break
            all_records.extend(data)
            if len(data) < records_per_page:
                break
        elif isinstance(data, dict):
            records = data.get("data", data.get("results", []))
            if not records:
                break
            all_records.extend(records)
            if len(records) < records_per_page:
                break
        else:
            break

        page += 1

    return all_records


def pull_timeseries(
    dataset_ids: list[int],
    date_from: str | None = None,
    date_to: str | None = None,
    batch_size: int = 50,
    timeout: int = 120,
) -> pd.DataFrame:
    """Pull Energy Aspects timeseries data and return one wide row per date."""
    all_dfs = []

    for i in range(0, len(dataset_ids), batch_size):
        batch = dataset_ids[i : i + batch_size]
        params = {
            "dataset_id": ",".join(str(dataset_id) for dataset_id in batch),
            "column_header": "dataset_id",
        }
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to

        response = get("/timeseries/csv", params=params, timeout=timeout)
        if not response.text.strip():
            continue

        df = pd.read_csv(StringIO(response.text))
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    result = all_dfs[0]
    for df in all_dfs[1:]:
        result = result.merge(df, on="Date", how="outer")

    result.rename(columns={"Date": "date"}, inplace=True)
    result["date"] = pd.to_datetime(result["date"])
    result.sort_values("date", inplace=True)
    result.reset_index(drop=True, inplace=True)
    result.columns = [str(column) for column in result.columns]

    return result


def normalize_postgres_identifier(
    name: str,
    max_length: int = POSTGRES_IDENTIFIER_MAX_LENGTH,
) -> str:
    identifier = re.sub(r"[^a-z0-9_]", "_", str(name).lower())
    identifier = re.sub(r"_+", "_", identifier).strip("_")

    if not identifier:
        identifier = "col"
    if identifier[0].isdigit():
        identifier = f"col_{identifier}"
    if len(identifier) <= max_length:
        return identifier

    digest = hashlib.sha1(identifier.encode("utf-8")).hexdigest()[:8]
    tail_length = min(16, max_length - len(digest) - 2)
    head_length = max_length - len(digest) - tail_length - 2

    prefix = identifier[:head_length].rstrip("_")
    suffix = identifier[-tail_length:].lstrip("_")

    return f"{prefix}_{suffix}_{digest}"


def make_unique_postgres_identifiers(
    names: list[str],
    max_length: int = POSTGRES_IDENTIFIER_MAX_LENGTH,
) -> list[str]:
    used: set[str] = set()
    safe_names: list[str] = []

    for name in names:
        base = normalize_postgres_identifier(name, max_length=max_length)
        candidate = base
        counter = 2

        while candidate in used:
            suffix = f"_{counter}"
            trimmed_base = base[: max_length - len(suffix)].rstrip("_")
            candidate = f"{trimmed_base}{suffix}"
            counter += 1

        used.add(candidate)
        safe_names.append(candidate)

    return safe_names


def make_postgres_safe_columns(
    df: pd.DataFrame,
    max_length: int = POSTGRES_IDENTIFIER_MAX_LENGTH,
) -> pd.DataFrame:
    safe_columns = make_unique_postgres_identifiers(
        [str(column) for column in df.columns],
        max_length=max_length,
    )
    if list(df.columns) == safe_columns:
        return df

    result = df.copy()
    result.columns = safe_columns
    return result
