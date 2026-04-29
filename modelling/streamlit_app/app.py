"""Streamlit operator console for the knn_model_only_load model family.

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
    _PAGES_DIR / "0_Home.py",
    title="Home",
    icon=":material/home:",
    default=True,
)

model_pages = [
    st.Page(_PAGES_DIR / "1_Data.py",       title="Data",       icon=":material/database:"),
    st.Page(_PAGES_DIR / "2_Configs.py",    title="Configs",    icon=":material/tune:"),
    st.Page(_PAGES_DIR / "3_Candidates.py", title="Candidates", icon=":material/search:"),
    st.Page(_PAGES_DIR / "4_Run.py",        title="Run",        icon=":material/play_arrow:"),
    st.Page(_PAGES_DIR / "5_Compare.py",    title="Compare",    icon=":material/compare:"),
]

fundies_pages = [
    st.Page(_PAGES_DIR / "6_Fundies_Outages.py",  title="Outages",  icon=":material/build:"),
    st.Page(_PAGES_DIR / "7_Fundies_Fuel_Mix.py", title="Fuel Mix", icon=":material/bolt:"),
]

pg = st.navigation(
    {
        "": [home],
        "Fundies": fundies_pages,
        "Model": model_pages,
    }
)
pg.run()
