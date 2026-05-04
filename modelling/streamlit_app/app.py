"""Streamlit fundies console (data inspection only).

The like_day_model_knn pages were removed when the model migrated to a
terminal-only pipeline (see
``da_models/like_day_model_knn/pjm_rto_hourly/pipelines/forecast_single_day.py``).
This app now hosts only the fundies (data-inspection) pages.

Run from ``modelling/streamlit_app``::

    streamlit run app.py
"""
from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

_APP_ROOT = Path(__file__).resolve().parent
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

st.set_page_config(
    page_title="Fundies Console",
    layout="wide",
)

_PAGES_DIR = _APP_ROOT / "pages"

home = st.Page(
    _PAGES_DIR / "Home.py",
    title="Home",
    icon=":material/home:",
    default=True,
)

_FUNDIES_PAGES = _PAGES_DIR / "fundies"

fundies_pages = [
    # Disabled — re-enable by uncommenting.
    # st.Page(_FUNDIES_PAGES / "Fundies_DA_RT_Settles.py",        title="DA vs RT LMP",         icon=":material/show_chart:"),
    st.Page(_FUNDIES_PAGES / "Fundies_Outages.py",              title="Outages",              icon=":material/build:"),
    st.Page(_FUNDIES_PAGES / "Fundies_Fuel_Mix.py",             title="Fuel Mix",             icon=":material/bolt:"),
    st.Page(_FUNDIES_PAGES / "Fundies_PJM_Net_Load.py",         title="PJM Net Load",         icon=":material/insights:"),
    st.Page(_FUNDIES_PAGES / "Fundies_PJM_Net_Load_Compare.py", title="PJM Compare Two Days", icon=":material/compare_arrows:"),
    st.Page(_FUNDIES_PAGES / "Fundies_Meteologica.py",          title="Meteologica",          icon=":material/cloud:"),
    st.Page(_FUNDIES_PAGES / "Fundies_Meteologica_Compare.py",  title="Meteo Compare",        icon=":material/compare_arrows:"),
]

pg = st.navigation(
    {
        "": [home],
        "Fundies": fundies_pages,
    }
)
pg.run()
