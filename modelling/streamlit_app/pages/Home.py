"""Landing page — store status and section overview."""
from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from lib import store  # noqa: E402

st.title("KNN Load-Only Console")
st.write(
    "Operator console for the `like_day_model_knn` model family — "
    "`per_day_daily_features`, `per_day_hourly_features`, `per_hour`."
)

st.markdown(
    """
    Use the sidebar to navigate:

    **Model**
    - **Data** — inspect load-forecast and DA-LMP inputs for a target date.
    - **Configs** — save and reuse named `KnnModelConfig` overrides.
    - **Candidates** — preview the eligible candidate pool for a date + config before running.
    - **Run** — launch backtests for a date and config across the three model variants.
    - **Compare** — pick N runs and compare analogs, contributions, and forecast curves side-by-side.

    **Fundies**
    - **Outages** — RTO forecast vintage heatmaps and seasonal overlays.
    """
)

st.divider()
st.caption(f"Analog store: `{store.store_dir()}`")
if store.store_has_data():
    runs = store.load_runs()
    st.caption(
        f"{len(runs)} runs · "
        f"{runs['model_name'].nunique()} model variants · "
        f"{runs['target_date'].nunique()} target dates"
    )
else:
    st.warning(
        "No analog store data found. Run one of the `single_day.py` scripts "
        "without `--skip-analog-store` to populate the store."
    )
