"""Manage named KnnModelConfig variants used by the Run page."""
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

_APP_ROOT = Path(__file__).resolve().parents[2]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from lib import config_io  # noqa: E402

st.title("Saved Configs")
st.caption(
    "Named overrides for `KnnModelConfig`. The Run page picks one of these "
    "(or the model defaults) when launching backtests."
)
st.caption(f"Stored at: `{config_io.CONFIGS_DIR}`")

existing = config_io.list_configs()

# ── Sidebar: which config to edit ────────────────────────────
st.sidebar.header("Editor")
edit_options = ["+ New config"] + [c["name"] for c in existing]
choice = st.sidebar.radio(
    "Config",
    edit_options,
    key="config_choice",
)

is_new = choice == "+ New config"
if is_new:
    payload = dict(config_io.DEFAULT_PAYLOAD)
else:
    payload = config_io.load_config(choice) or dict(config_io.DEFAULT_PAYLOAD)

# ── Existing configs table ────────────────────────────────────
st.subheader("Existing")
if existing:
    df = pd.DataFrame([
        {
            "name": c.get("name", ""),
            "description": c.get("description", ""),
            "n_analogs": c.get("n_analogs"),
            "season_window_days": c.get("season_window_days"),
            "min_pool_size": c.get("min_pool_size"),
            "flt_radius (per_hour)": c.get("per_hour", {}).get("flt_radius"),
            "updated_at_utc": c.get("updated_at_utc", ""),
        }
        for c in existing
    ])
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("No saved configs yet. Use the form below to create one.")

st.divider()

# ── Editor form ───────────────────────────────────────────────
st.subheader("New config" if is_new else f"Edit: {choice}")

with st.form("config_form"):
    name = st.text_input(
        "Name",
        value="" if is_new else payload.get("name", choice),
        help="Used as the filename (sanitized) and as the dropdown label on the Run page.",
        disabled=not is_new,
    )
    description = st.text_input(
        "Description",
        value=payload.get("description", ""),
    )

    st.markdown("**Common knobs**")
    cols = st.columns(3)
    n_analogs = cols[0].number_input(
        "n_analogs",
        min_value=1,
        max_value=200,
        value=int(payload.get("n_analogs", 20)),
    )
    season_window_days = cols[1].number_input(
        "season_window_days",
        min_value=7,
        max_value=365,
        value=int(payload.get("season_window_days", 60)),
    )
    min_pool_size = cols[2].number_input(
        "min_pool_size",
        min_value=10,
        max_value=500,
        value=int(payload.get("min_pool_size", 100)),
    )

    st.markdown("**per_hour knobs**")
    flt_radius = st.number_input(
        "flt_radius",
        min_value=0,
        max_value=6,
        value=int(payload.get("per_hour", {}).get("flt_radius", 1)),
        help="Half-width of the temporal feature window. Match window is HE±flt_radius.",
    )

    save_label = "Create" if is_new else "Save changes"
    submitted = st.form_submit_button(save_label, type="primary")

if submitted:
    if is_new and not name.strip():
        st.error("Name is required for a new config.")
    else:
        save_name = name.strip() if is_new else choice
        path = config_io.save_config(save_name, {
            "description": description,
            "n_analogs": n_analogs,
            "season_window_days": season_window_days,
            "min_pool_size": min_pool_size,
            "per_hour": {"flt_radius": flt_radius},
        })
        st.success(f"Saved `{path.name}`")
        if is_new:
            # Move sidebar selection to the newly-created config on rerun.
            st.session_state["config_choice"] = save_name
        st.rerun()

# Delete sits outside the form so it doesn't fight save semantics.
if not is_new:
    st.divider()
    if st.button(f"Delete `{choice}`", type="secondary"):
        if config_io.delete_config(choice):
            st.success(f"Deleted `{choice}`")
            st.session_state["config_choice"] = "+ New config"
            st.rerun()
