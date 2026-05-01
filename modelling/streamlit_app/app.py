"""Streamlit operator console for the like_day_model_knn model family.

Run from ``modelling/streamlit_app``::

    streamlit run app.py

Optional environment override::

    set KNN_ANALOG_STORE_DIR=<path>
"""
from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

_APP_ROOT = Path(__file__).resolve().parent
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

st.set_page_config(
    page_title="KNN Load-Only Console",
    layout="wide",
)

_PAGES_DIR = _APP_ROOT / "pages"

home = st.Page(
    _PAGES_DIR / "Home.py",
    title="Home",
    icon=":material/home:",
    default=True,
)

_MODELLING_PAGES = _PAGES_DIR / "modelling"
_FUNDIES_PAGES = _PAGES_DIR / "fundies"

model_pages = [
    st.Page(_MODELLING_PAGES / "Data.py",       title="Data",       icon=":material/database:"),
    st.Page(_MODELLING_PAGES / "Configs.py",    title="Configs",    icon=":material/tune:"),
    st.Page(_MODELLING_PAGES / "Candidates.py", title="Candidates", icon=":material/search:"),
    st.Page(_MODELLING_PAGES / "Run.py",        title="Run",        icon=":material/play_arrow:"),
    st.Page(_MODELLING_PAGES / "Compare.py",    title="Compare",    icon=":material/compare:"),
]

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
        "Model": model_pages,
    }
)
pg.run()
