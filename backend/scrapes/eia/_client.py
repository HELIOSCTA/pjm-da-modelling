"""Shared EIA Open Data API v2 client.

EIA's v2 API exposes data at category routes (e.g.
``/petroleum/pri/spt/data/``) and you select specific time series via
``facets[<facet>][]=<value>`` query params. The legacy ``/seriesid/``
shortcut works only for some categories -- coal market-index series
and most aggregate routes don't honour it -- so this client builds
proper route + facet queries.

Auth: requires ``EIA_API_KEY`` in ``backend/.env`` (free, register at
https://www.eia.gov/opendata/register.php).

Pagination: EIA caps each response at 5,000 rows. We page until the
result set is exhausted.

Not a runnable script.
"""

from __future__ import annotations

from typing import Iterable, Mapping

import pandas as pd
import requests

from backend import credentials

EIA_BASE = "https://api.eia.gov/v2"
EIA_TIMEOUT_SECONDS = 60
EIA_PAGE_SIZE = 5000


def _check_key() -> str:
    key = credentials.EIA_API_KEY
    if not key:
        raise RuntimeError(
            "EIA_API_KEY not set -- add to backend/.env "
            "(register at https://www.eia.gov/opendata/register.php)"
        )
    return key


def fetch_route_data(
    route: str,
    frequency: str,
    facets: Mapping[str, Iterable[str]] | None = None,
    data_fields: Iterable[str] = ("value",),
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Fetch rows from an EIA v2 ``{route}/data/`` endpoint.

    Parameters
    ----------
    route : path under ``/v2/``, e.g. ``"petroleum/pri/spt"`` or
        ``"coal/price-by-rank"``. No leading or trailing slashes.
    frequency : one of the route's published frequencies
        (``"daily"``, ``"weekly"``, ``"monthly"``, ``"annual"``).
    facets : mapping of facet-name -> iterable of facet values to
        select (e.g. ``{"series": ["RWTC", "RBRTE"]}``). Omit to get
        every row at that frequency.
    data_fields : data fields to return (almost always ``("value",)``).
    start, end : ISO date strings (``YYYY-MM-DD``); both inclusive.

    Returns
    -------
    DataFrame of EIA rows. Schema depends on the route -- all rows have
    ``period`` (datetime) and ``value`` (float) at minimum, plus any
    descriptive columns the route adds (``series``, ``product-name``,
    ``stateRegionId``, ``area-name``, ``units``, etc.).
    """
    key = _check_key()
    url = f"{EIA_BASE}/{route.strip('/')}/data/"

    base_params: dict[str, str | int] = {
        "api_key": key,
        "frequency": frequency,
        "length": EIA_PAGE_SIZE,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
    }
    for i, field in enumerate(data_fields):
        base_params[f"data[{i}]"] = field
    if start:
        base_params["start"] = start
    if end:
        base_params["end"] = end

    # Build facet params; EIA wants repeated facets[name][] entries.
    facet_params: list[tuple[str, str]] = []
    if facets:
        for name, values in facets.items():
            for v in values:
                facet_params.append((f"facets[{name}][]", v))

    offset = 0
    pages: list[pd.DataFrame] = []
    while True:
        params: list[tuple[str, str | int]] = list(base_params.items()) + facet_params
        params.append(("offset", offset))
        resp = requests.get(url, params=params, timeout=EIA_TIMEOUT_SECONDS)
        resp.raise_for_status()
        body = resp.json().get("response", {})
        rows = body.get("data", []) or []
        if not rows:
            break
        pages.append(pd.DataFrame(rows))
        if len(rows) < EIA_PAGE_SIZE:
            break
        offset += EIA_PAGE_SIZE

    if not pages:
        return pd.DataFrame()

    df = pd.concat(pages, ignore_index=True)
    df["period"] = pd.to_datetime(df["period"], errors="coerce")
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["period"]).reset_index(drop=True)
