"""Compare N analog runs side-by-side."""
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

_APP_ROOT = Path(__file__).resolve().parents[2]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from lib import store  # noqa: E402

st.title("Compare Runs")

if not store.store_has_data():
    st.info(
        "No analog store data found. Run one of the `single_day.py` scripts "
        "without `--skip-analog-store` to populate the store."
    )
    st.stop()

if st.sidebar.button("Refresh data"):
    store.clear_cache()
    st.rerun()

runs = store.load_runs()

# Consume any preselection handed over from the Run page.
preselect_run_ids: list[str] = st.session_state.pop("preselect_run_ids", []) or []
preselected_runs = runs[runs["run_id"].isin(preselect_run_ids)]
preselected_dates = preselected_runs["target_date"].drop_duplicates().tolist()

st.sidebar.header("Filter")
models = runs["model_name"].drop_duplicates().sort_values().tolist()
model_filter = st.sidebar.multiselect("Model", models, default=models)

dates = (
    runs["target_date"].drop_duplicates().sort_values(ascending=False).tolist()
)
default_dates = sorted(
    set(preselected_dates) | set(dates[: min(5, len(dates))]),
    reverse=True,
)
date_filter = st.sidebar.multiselect("Target date", dates, default=default_dates)

filtered = runs[
    runs["model_name"].isin(model_filter)
    & runs["target_date"].isin(date_filter)
].copy()

if len(filtered) == 0:
    st.info("No runs match the current filters.")
    st.stop()

filtered["label"] = (
    filtered["model_name"]
    + " | "
    + filtered["target_date"].astype(str)
    + " | "
    + filtered["run_id"].str.slice(0, 8)
    + " | "
    + filtered["created_at_utc"].astype(str)
)

st.sidebar.header("Selection")
if preselect_run_ids:
    default_selection = filtered.loc[
        filtered["run_id"].isin(preselect_run_ids), "label"
    ].tolist()
else:
    default_selection = filtered["label"].tolist()[: min(2, len(filtered))]
selected_labels = st.sidebar.multiselect(
    "Runs to compare",
    filtered["label"].tolist(),
    default=default_selection,
    help="Pick 1+ runs. Order in the table/chart matches selection order.",
)

if not selected_labels:
    st.info("Pick one or more runs from the sidebar to begin.")
    st.stop()

selected_run_ids = (
    filtered.loc[filtered["label"].isin(selected_labels), "run_id"].tolist()
)
selected_runs = (
    runs[runs["run_id"].isin(selected_run_ids)]
    .copy()
    .reset_index(drop=True)
)
selected_runs["short_id"] = selected_runs["run_id"].str.slice(0, 8)
selected_runs["display"] = (
    selected_runs["model_name"]
    + " · "
    + selected_runs["target_date"].astype(str)
    + " · "
    + selected_runs["short_id"]
)
display_by_run_id = dict(
    zip(selected_runs["run_id"], selected_runs["display"])
)

st.subheader("Run Summary")
summary_cols = [
    "display",
    "target_date",
    "model_name",
    "match_unit",
    "n_analogs",
    "n_candidates",
    "n_pool",
    "season_window_days",
    "min_pool_size",
    "flt_radius",
    "hub",
    "description",
    "created_at_utc",
    "run_id",
]
st.dataframe(
    selected_runs[[c for c in summary_cols if c in selected_runs.columns]],
    use_container_width=True,
    hide_index=True,
)

st.subheader("Forecast Curves (Rebuilt from Analog Contributions)")
contrib = store.load_contributions(selected_run_ids)
if len(contrib) == 0:
    st.warning("No hourly contributions found for the selected runs.")
else:
    forecasts = (
        contrib.groupby(["run_id", "hour_ending"], as_index=False)[
            "lmp_contribution"
        ]
        .sum()
        .rename(columns={"lmp_contribution": "forecast_lmp"})
    )
    forecasts["display"] = forecasts["run_id"].map(display_by_run_id)
    fig = px.line(
        forecasts,
        x="hour_ending",
        y="forecast_lmp",
        color="display",
        markers=True,
        labels={
            "hour_ending": "Hour ending",
            "forecast_lmp": "Point forecast LMP ($/MWh)",
            "display": "Run",
        },
    )
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Analog Picks per Run")
picks = store.load_picks(selected_run_ids)
if len(picks) == 0:
    st.warning("No analog picks for the selected runs.")
else:
    tabs = st.tabs(selected_runs["display"].tolist())
    for tab, (_, run_row) in zip(tabs, selected_runs.iterrows()):
        with tab:
            run_picks = picks[picks["run_id"] == run_row["run_id"]].copy()
            if len(run_picks) == 0:
                st.warning("This run selected no analogs.")
                continue

            display_cols = [
                "hour_ending",
                "rank",
                "analog_date",
                "distance",
                "weight_pct",
                "top_distance_group",
            ]
            display_cols += [
                c for c in run_picks.columns if c.startswith("distance_")
            ]
            st.dataframe(
                run_picks[
                    [c for c in display_cols if c in run_picks.columns]
                ],
                use_container_width=True,
                hide_index=True,
            )

            chart_df = run_picks
            if str(run_row["match_unit"]) == "hour":
                hour_options = sorted(
                    chart_df["hour_ending"].dropna().astype(int).unique().tolist()
                )
                hour = st.selectbox(
                    "Hour ending",
                    hour_options,
                    key=f"hour_{run_row['run_id']}",
                )
                chart_df = chart_df[
                    chart_df["hour_ending"].astype("Int64") == int(hour)
                ]

            fig = px.bar(
                chart_df.sort_values("weight_pct", ascending=True),
                x="weight_pct",
                y="analog_date",
                color=(
                    "top_distance_group"
                    if "top_distance_group" in chart_df.columns
                    else None
                ),
                orientation="h",
                hover_data=[
                    c
                    for c in ["hour_ending", "rank", "distance"]
                    if c in chart_df.columns
                ],
                labels={
                    "analog_date": "Analog date",
                    "weight_pct": "Forecast weight",
                    "top_distance_group": "Largest distance group",
                },
            )
            fig.update_xaxes(tickformat=".1%")
            st.plotly_chart(fig, use_container_width=True)

st.subheader("Analog Date Overlap")
if len(picks) > 1 and len(selected_run_ids) > 1:
    overlap = (
        picks.groupby("analog_date")["run_id"]
        .nunique()
        .reset_index(name="n_runs")
        .sort_values(["n_runs", "analog_date"], ascending=[False, False])
    )
    overlap = overlap[overlap["n_runs"] > 1]
    if len(overlap) == 0:
        st.caption("No analog dates were picked by more than one selected run.")
    else:
        st.caption(
            f"{len(overlap)} analog dates were picked by 2+ runs "
            f"(of {len(selected_run_ids)} selected)."
        )
        st.dataframe(overlap, use_container_width=True, hide_index=True)
