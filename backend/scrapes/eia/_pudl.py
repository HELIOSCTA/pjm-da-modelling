"""Shared PUDL S3 reader.

PUDL (Public Utility Data Liberation) publishes cleaned, conformed
parquet snapshots of every EIA / FERC / EPA filing to an anonymous-
read S3 bucket at ``s3://pudl.catalyst.coop/{stable,nightly}/``. We
use it as the canonical access path for EIA-923 form data
(monthly fuel receipts + per-delivery raw) because PUDL handles the
ID harmonization, type coercion, and unit normalization that the
EIA Open Data API does not.

Channel: ``stable`` (last quarterly release) is the default for
production pipelines; ``nightly`` is the bleeding-edge build that
may include schema churn.

Not a runnable script.
"""

from __future__ import annotations

from typing import Iterable, Literal

import pandas as pd
import pyarrow.parquet as pq
from pyarrow import fs as pafs

PUDL_S3_BUCKET = "pudl.catalyst.coop"
PUDL_S3_REGION = "us-west-2"


def read_pudl_table(
    table_name: str,
    columns: Iterable[str] | None = None,
    channel: Literal["stable", "nightly"] = "stable",
    filters: list | None = None,
) -> pd.DataFrame:
    """Read a single PUDL parquet table.

    Parameters
    ----------
    table_name : the ``*.parquet`` filename without extension (e.g.
        ``"out_eia923__monthly_fuel_receipts_costs"``).
    columns : optional column subset.
    channel : ``"stable"`` or ``"nightly"``.
    filters : pyarrow filter expressions for push-down (e.g.
        ``[("state", "in", ["PA", "NJ"])]``).
    """
    fs = pafs.S3FileSystem(region=PUDL_S3_REGION, anonymous=True)
    path = f"{PUDL_S3_BUCKET}/{channel}/{table_name}.parquet"
    table = pq.read_table(
        path,
        filesystem=fs,
        columns=list(columns) if columns else None,
        filters=filters,
    )
    return table.to_pandas()
