"""Launch single-day backtests for the like_day_model_knn variants."""
from __future__ import annotations

import traceback
from datetime import date, timedelta
from pathlib import Path
import sys
from typing import Any, Callable

import streamlit as st

_APP_ROOT = Path(__file__).resolve().parents[1]
_MODELLING_ROOT = _APP_ROOT.parent
for path in (_APP_ROOT, _MODELLING_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from lib import config_io, store  # noqa: E402
from lib.ui import init_target_date_state  # noqa: E402

from da_models.like_day_model_knn import configs as knn_configs  # noqa: E402
from da_models.like_day_model_knn.per_day_daily_features import single_day as pddf_sd  # noqa: E402
from da_models.like_day_model_knn.per_day_hourly_features import single_day as pdhf_sd  # noqa: E402
from da_models.like_day_model_knn.per_hour import single_day as ph_sd  # noqa: E402

DEFAULTS_LABEL = "(model defaults)"
PER_DAY_KEYS = ("n_analogs", "season_window_days", "min_pool_size")
PER_HOUR_KEYS = PER_DAY_KEYS + ("flt_radius",)

st.title("Run a Single-Day Backtest")

init_target_date_state()
st.sidebar.header("Inputs")
target_date: date = st.sidebar.date_input(
    "Target date",
    key="target_date",
)

st.sidebar.subheader("Config")
saved = config_io.list_configs()
saved_names = [c["name"] for c in saved]
choice = st.sidebar.selectbox(
    "Use config",
    [DEFAULTS_LABEL] + saved_names,
    help="Pick a saved config from the Configs page, or use the model defaults.",
)
selected_config: dict[str, Any] | None = (
    None if choice == DEFAULTS_LABEL
    else next((c for c in saved if c["name"] == choice), None)
)
all_overrides = config_io.overrides_for(selected_config)

st.sidebar.subheader("Model variants")
run_pddf = st.sidebar.checkbox("per_day_daily_features", value=True)
run_pdhf = st.sidebar.checkbox("per_day_hourly_features", value=True)
run_ph = st.sidebar.checkbox("per_hour", value=True)

Plan = tuple[str, Callable[..., Path], dict[str, Any]]
plans: list[Plan] = []
if run_pddf:
    plans.append((
        "per_day_daily_features",
        pddf_sd.generate,
        {k: all_overrides[k] for k in PER_DAY_KEYS if k in all_overrides},
    ))
if run_pdhf:
    plans.append((
        "per_day_hourly_features",
        pdhf_sd.generate,
        {k: all_overrides[k] for k in PER_DAY_KEYS if k in all_overrides},
    ))
if run_ph:
    ph_kwargs = {k: all_overrides[k] for k in PER_HOUR_KEYS if k in all_overrides}
    if "flt_radius" not in ph_kwargs:
        ph_kwargs["flt_radius"] = int(knn_configs.PER_HOUR_SPEC.flt_radius)
    plans.append(("per_hour", ph_sd.generate, ph_kwargs))

st.write(f"Target date: **{target_date}**")
st.write(
    "Models: **"
    + (", ".join(name for name, _, _ in plans) if plans else "(none selected)")
    + "**"
)
st.write(f"Config: **{choice}**")

if selected_config is not None:
    cols = st.columns(4)
    cols[0].metric("n_analogs", selected_config.get("n_analogs"))
    cols[1].metric("season_window_days", selected_config.get("season_window_days"))
    cols[2].metric("min_pool_size", selected_config.get("min_pool_size"))
    cols[3].metric("flt_radius", selected_config.get("per_hour", {}).get("flt_radius"))
    if selected_config.get("description"):
        st.caption(selected_config["description"])
else:
    st.caption("Using `KnnModelConfig` defaults.")

st.caption(f"Analog store: `{store.store_dir()}`")

if "last_run_results" not in st.session_state:
    st.session_state.last_run_results = []

run_clicked = st.button("Run", type="primary", disabled=not plans)

if run_clicked:
    runs_before = (
        set(store.load_runs()["run_id"].tolist()) if store.store_has_data() else set()
    )

    results: list[dict[str, Any]] = []
    for name, fn, extra in plans:
        with st.spinner(f"Running {name} for {target_date}..."):
            try:
                output_path = fn(
                    target_date=target_date,
                    analog_store_dir=store.store_dir(),
                    write_analog_store=True,
                    **extra,
                )
                results.append({
                    "model": name,
                    "ok": True,
                    "output_path": str(output_path),
                    "error": None,
                })
                st.success(f"{name}: saved `{Path(output_path).name}`")
            except Exception as exc:
                results.append({
                    "model": name,
                    "ok": False,
                    "output_path": None,
                    "error": f"{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}",
                })
                st.error(f"{name}: failed — {type(exc).__name__}: {exc}")

    store.clear_cache()
    runs_after = (
        set(store.load_runs()["run_id"].tolist()) if store.store_has_data() else set()
    )
    new_run_ids = sorted(runs_after - runs_before)
    st.session_state.last_run_results = results
    st.session_state.last_new_run_ids = new_run_ids

if st.session_state.last_run_results:
    st.divider()
    st.subheader("Last Run")
    for r in st.session_state.last_run_results:
        if r["ok"]:
            st.write(f"✓ **{r['model']}** — `{r['output_path']}`")
        else:
            with st.expander(
                f"✗ {r['model']} — {r['error'].splitlines()[0]}",
                expanded=False,
            ):
                st.code(r["error"])

    new_ids = st.session_state.get("last_new_run_ids", [])
    if new_ids:
        st.write(f"New run_ids written to the analog store: **{len(new_ids)}**")
        st.code("\n".join(new_ids))
        if st.button("Open in Compare →", type="primary"):
            st.session_state["preselect_run_ids"] = new_ids
            st.switch_page("pages/5_Compare.py")
