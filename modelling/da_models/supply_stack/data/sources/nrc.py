"""NRC daily reactor power status reader.

Pulls the public pipe-delimited daily power status feed the NRC
publishes for the trailing 365 days. Each row is one (Unit, Date,
Power%) -- 0-100% utilization of a single reactor unit.

Includes a hand-curated PJM-unit -> EIA crosswalk for the 31 PJM
reactor units (NRC unit names don't match EIA ``plant_name_eia``).

Not a runnable script.
"""

from __future__ import annotations

import io
import urllib.request

import pandas as pd

NRC_URL = (
    "https://www.nrc.gov/reading-rm/doc-collections/event-status/"
    "reactor-status/powerreactorstatusforlast365days.txt"
)
NRC_TIMEOUT_SECONDS = 30


# PJM nuclear unit crosswalk: NRC ``Unit`` name -> EIA plant_id / generator_id.
# Capacities from EIA-860 summer_capacity_mw. Mirror of the
# helioscta-pjm-da supply_stack_model crosswalk; refresh when units come
# online or are uprated.
PJM_NRC_CROSSWALK: dict[str, dict] = {
    # Peach Bottom (PA) -- plant 3166
    "Peach Bottom 2": {"plant_id_eia": 3166, "generator_id": "2", "capacity_mw": 1265},
    "Peach Bottom 3": {"plant_id_eia": 3166, "generator_id": "3", "capacity_mw": 1285},
    # Susquehanna (PA) -- plant 6103
    "Susquehanna 1": {"plant_id_eia": 6103, "generator_id": "1", "capacity_mw": 1247},
    "Susquehanna 2": {"plant_id_eia": 6103, "generator_id": "2", "capacity_mw": 1247},
    # Braidwood (IL) -- plant 6022
    "Braidwood 1": {"plant_id_eia": 6022, "generator_id": "1", "capacity_mw": 1183},
    "Braidwood 2": {"plant_id_eia": 6022, "generator_id": "2", "capacity_mw": 1149},
    # Byron (IL) -- plant 6023
    "Byron 1": {"plant_id_eia": 6023, "generator_id": "1", "capacity_mw": 1164},
    "Byron 2": {"plant_id_eia": 6023, "generator_id": "2", "capacity_mw": 1136},
    # Salem (NJ) -- plant 2410
    "Salem 1": {"plant_id_eia": 2410, "generator_id": "1", "capacity_mw": 1146},
    "Salem 2": {"plant_id_eia": 2410, "generator_id": "2", "capacity_mw": 1139},
    # LaSalle (IL) -- plant 6026
    "LaSalle 1": {"plant_id_eia": 6026, "generator_id": "1", "capacity_mw": 1130},
    "LaSalle 2": {"plant_id_eia": 6026, "generator_id": "2", "capacity_mw": 1134},
    # Limerick (PA) -- plant 6105
    "Limerick 1": {"plant_id_eia": 6105, "generator_id": "1", "capacity_mw": 1120},
    "Limerick 2": {"plant_id_eia": 6105, "generator_id": "2", "capacity_mw": 1122},
    # D.C. Cook (MI) -- plant 6000
    "D.C. Cook 1": {"plant_id_eia": 6000, "generator_id": "1", "capacity_mw": 1009},
    "D.C. Cook 2": {"plant_id_eia": 6000, "generator_id": "2", "capacity_mw": 1168},
    # North Anna (VA) -- plant 6168
    "North Anna 1": {"plant_id_eia": 6168, "generator_id": "1", "capacity_mw": 948},
    "North Anna 2": {"plant_id_eia": 6168, "generator_id": "2", "capacity_mw": 944},
    # Quad Cities (IL) -- plant 880
    "Quad Cities 1": {"plant_id_eia": 880, "generator_id": "1", "capacity_mw": 908},
    "Quad Cities 2": {"plant_id_eia": 880, "generator_id": "2", "capacity_mw": 911},
    # Beaver Valley (PA) -- plant 6040
    "Beaver Valley 1": {"plant_id_eia": 6040, "generator_id": "1", "capacity_mw": 907},
    "Beaver Valley 2": {"plant_id_eia": 6040, "generator_id": "2", "capacity_mw": 901},
    # Dresden (IL) -- plant 869
    "Dresden 2": {"plant_id_eia": 869, "generator_id": "2", "capacity_mw": 902},
    "Dresden 3": {"plant_id_eia": 869, "generator_id": "3", "capacity_mw": 895},
    # Calvert Cliffs (MD) -- plant 6011
    "Calvert Cliffs 1": {"plant_id_eia": 6011, "generator_id": "1", "capacity_mw": 884},
    "Calvert Cliffs 2": {"plant_id_eia": 6011, "generator_id": "2", "capacity_mw": 861},
    # Surry (VA) -- plant 3806
    "Surry 1": {"plant_id_eia": 3806, "generator_id": "1", "capacity_mw": 838},
    "Surry 2": {"plant_id_eia": 3806, "generator_id": "2", "capacity_mw": 838},
    # Perry (OH) -- plant 6020
    "Perry 1": {"plant_id_eia": 6020, "generator_id": "1", "capacity_mw": 1240},
    # Hope Creek (NJ) -- plant 6118
    "Hope Creek 1": {"plant_id_eia": 6118, "generator_id": "1", "capacity_mw": 1174},
    # Davis-Besse (OH) -- plant 6149
    "Davis-Besse": {"plant_id_eia": 6149, "generator_id": "1", "capacity_mw": 894},
    # Three Mile Island (PA) -- plant 8011
    "Three Mile Island 1": {
        "plant_id_eia": 8011,
        "generator_id": "1",
        "capacity_mw": 803,
    },
}


def pull_reactor_status() -> pd.DataFrame:
    """Pull the trailing 365 days of NRC reactor power status.

    Returns one row per (Unit, ReportDt) with columns: ``Unit``,
    ``ReportDt`` (datetime), ``Power`` (int 0-100). Filter on
    ``Unit in PJM_NRC_CROSSWALK`` to scope to PJM.
    """
    response = urllib.request.urlopen(NRC_URL, timeout=NRC_TIMEOUT_SECONDS)  # noqa: S310
    text = response.read().decode("utf-8")
    df = pd.read_csv(io.StringIO(text), sep="|")
    df["ReportDt"] = pd.to_datetime(df["ReportDt"], format="mixed")
    df["Power"] = pd.to_numeric(df["Power"], errors="coerce").fillna(0).astype(int)
    return df


def filter_pjm(nrc_df: pd.DataFrame) -> pd.DataFrame:
    """Filter NRC frame to PJM reactors and enrich with crosswalk fields."""
    pjm = nrc_df[nrc_df["Unit"].isin(PJM_NRC_CROSSWALK)].copy()
    pjm["plant_id_eia"] = pjm["Unit"].map(
        lambda u: PJM_NRC_CROSSWALK[u]["plant_id_eia"]
    )
    pjm["generator_id"] = pjm["Unit"].map(
        lambda u: PJM_NRC_CROSSWALK[u]["generator_id"]
    )
    pjm["unit_capacity_mw"] = pjm["Unit"].map(
        lambda u: PJM_NRC_CROSSWALK[u]["capacity_mw"]
    )
    pjm["effective_mw"] = (pjm["unit_capacity_mw"] * pjm["Power"] / 100.0).round(1)
    pjm["date"] = pjm["ReportDt"].dt.date
    return pjm
