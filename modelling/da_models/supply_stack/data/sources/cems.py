"""EPA CEMS hourly emissions reader (via PUDL).

Reads ``core_epacems__hourly_emissions`` from the PUDL S3 bucket. CEMS
(Continuous Emissions Monitoring System) data covers every fossil
generating unit in the US with an EPA-mandated monitor and gives
hourly observed ``gross_load_mw``, ``heat_content_mmbtu``, and
``operating_time_hours`` -- the canonical source for empirically-derived
heat rates and capacity factors. Nuclear/hydro/wind/solar units are
not in CEMS (no emissions to monitor).

The full national table is hundreds of millions of rows; we always
filter on read by ``plant_id_eia`` (the matched PJM fleet) and ``year``.

Not a runnable script.
"""

from __future__ import annotations

from typing import Iterable, Literal

import pandas as pd
import pyarrow.parquet as pq
from pyarrow import fs as pafs

PUDL_S3_BUCKET = "pudl.catalyst.coop"
PUDL_S3_REGION = "us-west-2"
CEMS_TABLE = "core_epacems__hourly_emissions"

CEMS_COLUMNS: list[str] = [
    "plant_id_eia",
    "operating_datetime_utc",
    "gross_load_mw",
    "heat_content_mmbtu",
    "operating_time_hours",
    "state",
    "year",
]


def pull_hourly_emissions(
    plant_ids: Iterable[int],
    year: int,
    channel: Literal["stable", "nightly"] = "stable",
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Pull CEMS hourly emissions for a list of plants for one year.

    Always-filters on ``plant_id_eia`` (push-down predicate) so the
    reader only fetches the partitions for these plants. Pass the
    gas/coal/oil plant_id_eia list from
    ``artifacts/pudl_generators_audit.parquet``.
    """
    fs = pafs.S3FileSystem(region=PUDL_S3_REGION, anonymous=True)
    path = f"{PUDL_S3_BUCKET}/{channel}/{CEMS_TABLE}.parquet"
    ids = sorted(int(x) for x in plant_ids)
    cols = columns or CEMS_COLUMNS
    table = pq.read_table(
        path,
        filesystem=fs,
        columns=cols,
        filters=[("plant_id_eia", "in", ids), ("year", "==", int(year))],
    )
    df = table.to_pandas()
    df["operating_datetime_utc"] = pd.to_datetime(df["operating_datetime_utc"])
    return df
