import json
import threading
import time
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend import credentials

CONFIG_DIR = Path(__file__).parent

WSI_TRADER_CITY_IDS_FILEPATH = CONFIG_DIR / "wsi_trader_city_ids.json"

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_READ_TIMEOUT_SECONDS = 120
DEFAULT_TIMEOUT = (DEFAULT_CONNECT_TIMEOUT_SECONDS, DEFAULT_READ_TIMEOUT_SECONDS)
DEFAULT_RETRY_TOTAL = 3
DEFAULT_RETRY_BACKOFF_FACTOR = 1.0
DEFAULT_RETRY_STATUS_FORCELIST = (429, 500, 502, 503, 504)
DEFAULT_MIN_INTERVAL_SECONDS = 0.5
SENSITIVE_QUERY_KEYS = {"Account", "Profile", "Password"}


def _get_wsi_trader_credentials() -> dict[str, str]:

    return {
        "Account": credentials.WSI_TRADER_USERNAME,
        "Profile": credentials.WSI_TRADER_NAME,
        "Password": credentials.WSI_TRADER_PASSWORD,
    }


def _sanitize_params_for_logging(params_dict: dict[str, Any]) -> dict[str, Any]:

    sanitized: dict[str, Any] = {}
    for key, value in params_dict.items():
        sanitized[key] = "***" if key in SENSITIVE_QUERY_KEYS else value
    return sanitized


def _get_sanitized_request_context(
        base_url: str,
        params_dict: dict[str, Any] | None = None,
    ) -> dict[str, Any]:

    merged_params = {**_get_wsi_trader_credentials(), **(params_dict or {})}
    return {
        "base_url": base_url,
        "params": _sanitize_params_for_logging(merged_params),
    }


class _WsiTraderHttpClient:
    """
    Shared WSI HTTP client with retry, timeout, and request throttling.
    """

    def __init__(
            self,
            timeout: tuple[int, int] = DEFAULT_TIMEOUT,
            min_interval_seconds: float = DEFAULT_MIN_INTERVAL_SECONDS,
        ) -> None:
        self.timeout = timeout
        self.min_interval_seconds = min_interval_seconds
        self._last_request_monotonic = 0.0
        self._throttle_lock = threading.Lock()
        self.session = requests.Session()
        retries = Retry(
            total=DEFAULT_RETRY_TOTAL,
            connect=DEFAULT_RETRY_TOTAL,
            read=DEFAULT_RETRY_TOTAL,
            status=DEFAULT_RETRY_TOTAL,
            backoff_factor=DEFAULT_RETRY_BACKOFF_FACTOR,
            status_forcelist=DEFAULT_RETRY_STATUS_FORCELIST,
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _throttle(self) -> None:
        if self.min_interval_seconds <= 0:
            return

        with self._throttle_lock:
            elapsed = time.monotonic() - self._last_request_monotonic
            if elapsed < self.min_interval_seconds:
                time.sleep(self.min_interval_seconds - elapsed)
            self._last_request_monotonic = time.monotonic()

    def get(
            self,
            base_url: str,
            params_dict: dict[str, Any] | None = None,
            timeout: tuple[int, int] | None = None,
        ) -> requests.Response:
        merged_params = {**_get_wsi_trader_credentials(), **(params_dict or {})}
        self._throttle()
        response = self.session.get(
            base_url,
            params=merged_params,
            timeout=timeout or self.timeout,
        )
        response.raise_for_status()
        return response


_WSI_HTTP_CLIENT = _WsiTraderHttpClient()


def _read_csv_from_content(
        content: str,
        skiprows: int = 0,
        **read_csv_kwargs: Any,
    ) -> pd.DataFrame:
    try:
        return pd.read_csv(StringIO(content), skiprows=skiprows, **read_csv_kwargs)
    except pd.errors.EmptyDataError as exc:
        raise ValueError("WSI response contained no CSV data.") from exc
    except pd.errors.ParserError as exc:
        raise ValueError(f"Failed to parse WSI CSV response: {exc}") from exc


def _validate_dataframe(
        df: pd.DataFrame,
        context: str,
        required_columns: list[str] | None = None,
        allow_empty: bool = False,
    ) -> None:
    if not allow_empty and df.empty:
        raise ValueError(f"WSI response returned 0 rows for {context}.")

    if required_columns:
        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            raise ValueError(
                f"WSI response missing required columns for {context}. "
                f"Missing={missing_columns}, Actual={df.columns.tolist()}"
            )


def _pull_wsi_trader_text_data(
        base_url: str,
        params_dict: dict[str, Any] | None = None,
        min_lines: int = 1,
        allow_empty: bool = False,
    ) -> str:
    response = _WSI_HTTP_CLIENT.get(base_url=base_url, params_dict=params_dict)
    try:
        content = response.content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"WSI response decoding failed for {base_url}: {exc}") from exc

    if not allow_empty and not content.strip():
        raise ValueError(f"WSI response body is empty for {base_url}.")

    if min_lines > 0 and len(content.splitlines()) < min_lines:
        raise ValueError(
            f"WSI response has too few lines for {base_url}. "
            f"Expected at least {min_lines}, got {len(content.splitlines())}."
        )

    return content


def _pull_wsi_trader_csv_data(
        base_url: str,
        params_dict: dict[str, Any] | None = None,
        skiprows: int = 0,
        required_columns: list[str] | None = None,
        allow_empty: bool = False,
        **read_csv_kwargs: Any,
    ) -> pd.DataFrame:

    content = _pull_wsi_trader_text_data(
        base_url=base_url,
        params_dict=params_dict,
        min_lines=1,
        allow_empty=allow_empty,
    )
    df = _read_csv_from_content(content=content, skiprows=skiprows, **read_csv_kwargs)
    _validate_dataframe(
        df=df,
        context=base_url,
        required_columns=required_columns,
        allow_empty=allow_empty,
    )
    return df


def _get_wsi_site_ids(
        json_filepath: str = WSI_TRADER_CITY_IDS_FILEPATH,
    ) -> tuple[dict, list[str], list[str], list[str]]:

    with open(json_filepath, "r") as f:
        wsi_trader_city_ids: dict = json.load(f)

    regions = list(wsi_trader_city_ids.keys())

    site_ids, station_names = [], []
    for region in regions:
        site_ids.extend(list(wsi_trader_city_ids[region].keys()))
        station_names.extend(list(wsi_trader_city_ids[region].values()))

    return wsi_trader_city_ids, regions, site_ids, station_names
