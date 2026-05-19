"""yes_energy.objects feed reader and plant-name matcher.

Pulls the PJM plant dimension from ``yes_energy.objects`` on AWS RDS
and matches parquet ``plant`` strings to ``lpi_objectid`` by normalized
name. Consumed by ``validators/verify_vs_yes_energy_identity.py`` and
``validators/verify_vs_yes_energy_operations.py``.

Not a runnable script -- no ``__main__`` block.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[5]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from backend.utils.aws_postgresql_utils import pull_from_db  # noqa: E402

_PLANTS_QUERY = """
    SELECT
        lpi_objectid, plant_objectid, object_name, plant_name,
        primary_fuel, technology, secondary_technology,
        plant_capacity_mw, max_cap_mw,
        source_zone
    FROM yes_energy.objects
    WHERE iso = 'PJMISO'
      AND object_type = 'PLANT'
"""

_YE_COLS: tuple[str, ...] = (
    "lpi_objectid",
    "plant_objectid",
    "object_name",
    "primary_fuel",
    "technology",
    "secondary_technology",
    "plant_capacity_mw",
    "max_cap_mw",
    "source_zone",
)


def normalize_plant_name(name: str) -> str:
    """Lowercase, strip parens (location qualifiers), collapse whitespace.

    Conservative on purpose -- we don't want to over-match. e.g.::

        "Joliet 29 (Will, IL)"  -> "joliet 29"
        "Christiana (New Castle, DE)" -> "christiana"
        "Brandon Shores / Wagner" -> "brandon shores / wagner"
    """
    if not isinstance(name, str):
        return ""
    n = name.lower()
    n = re.sub(r"\([^)]*\)", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def pull_yes_energy_pjm_plants() -> pd.DataFrame:
    """Return the PJMISO PLANT rows from yes_energy.objects, with normalized names."""
    df = pull_from_db(_PLANTS_QUERY)
    if df is None:
        raise RuntimeError(
            "yes_energy pull failed -- check AWS credentials / backend/.env"
        )
    df["norm_object_name"] = df["object_name"].map(normalize_plant_name)
    df["norm_plant_name"] = df["plant_name"].map(normalize_plant_name)
    return df


def match_fleet_to_yes_energy(fleet: pd.DataFrame) -> pd.DataFrame:
    """Return ``fleet`` with ``ye_*`` columns left-joined from yes_energy.objects.

    Matches on normalized plant string -- tries ``object_name`` first,
    then ``plant_name``. Unmatched rows get NaN in the ``ye_*`` columns.
    Adds a ``norm_plant`` column for downstream inspection.
    """
    yes_energy = pull_yes_energy_pjm_plants()

    lookup = pd.concat(
        [
            yes_energy[["norm_object_name", *_YE_COLS]].rename(
                columns={"norm_object_name": "norm_name"}
            ),
            yes_energy[["norm_plant_name", *_YE_COLS]].rename(
                columns={"norm_plant_name": "norm_name"}
            ),
        ],
        ignore_index=True,
    )
    lookup = lookup.dropna(subset=["norm_name"])
    lookup = lookup[lookup["norm_name"] != ""]
    lookup = lookup.drop_duplicates(subset=["norm_name"], keep="first")
    lookup = lookup.rename(columns={c: f"ye_{c}" for c in _YE_COLS})

    out = fleet.copy()
    out["norm_plant"] = out["plant"].map(normalize_plant_name)
    out = out.merge(lookup, left_on="norm_plant", right_on="norm_name", how="left")
    out = out.drop(columns=["norm_name"])
    return out
