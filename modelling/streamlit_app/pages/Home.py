"""Landing page — fundies (data inspection) only.

The like_day_model_knn pages were removed when the model migrated to a
terminal-only pipeline. To run the model use::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day --date YYYY-MM-DD
"""
from __future__ import annotations

import streamlit as st

st.title("Fundies Console")
st.write(
    "Data-inspection pages for the PJM upstream feeds. "
    "The KNN model now runs as a terminal-output pipeline — see the "
    "module docstring for the invocation."
)

st.markdown(
    """
    **Fundies**
    - **Outages** — RTO forecast vintage heatmaps and seasonal overlays.
    - **Fuel Mix** — PJM hourly generation by fuel.
    - **PJM Net Load** — net-load forecast vs RT actuals.
    - **PJM Compare Two Days** — side-by-side net-load comparison.
    - **Meteologica** — alt-vendor net-load forecast.
    - **Meteo Compare** — side-by-side Meteologica comparison.
    """
)
