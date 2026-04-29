"""Streamlit explorer for the knn_model_only_load analog Parquet store.

Run from ``modelling/da_models/knn_model_only_load``:

    streamlit run .\streamlit_analog_explorer.py
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

STORE_DIR = Path(
    os.getenv(
        "KNN_ANALOG_STORE_DIR",
        str(Path(__file__).resolve().parent / "output" / "analog_store"),
    ),
)


def _glob_path(*parts: str) -> str:
    return STORE_DIR.joinpath(*parts).as_posix()


RUNS = _glob_path("runs", "*.parquet")
PICKS = _glob_path("analog_picks", "*.parquet")
TRACE = _glob_path("analog_feature_trace", "*.parquet")
CONTRIB = _glob_path("hourly_contributions", "*.parquet")
CORR = _glob_path("feature_price_correlations", "*.parquet")


def _store_has_data() -> bool:
    return any((STORE_DIR / "runs").glob("*.parquet"))


@st.cache_data(show_spinner=False)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with duckdb.connect() as con:
        return con.execute(sql, params).df()


def main() -> None:
    st.set_page_config(page_title="KNN Load-Only Analog Explorer", layout="wide")
    st.title("KNN Load-Only Analog Explorer")

    if not _store_has_data():
        st.info(
            "No analog store data found. Run one of the single_day.py scripts "
            "without --skip-analog-store to create output/analog_store/*.parquet."
        )
        return

    runs = query(f"""
        select *
        from read_parquet('{RUNS}')
        order by target_date desc, model_name, created_at_utc desc
    """)

    model_names = runs["model_name"].drop_duplicates().sort_values().tolist()
    model_name = st.sidebar.selectbox("Model", model_names)

    model_runs = runs[runs["model_name"] == model_name]
    target_dates = model_runs["target_date"].drop_duplicates().tolist()
    target_date = st.sidebar.selectbox("Target date", target_dates)

    run_options = model_runs[model_runs["target_date"] == target_date].copy()
    run_options["label"] = (
        run_options["created_at_utc"].astype(str)
        + " | "
        + run_options["run_id"].str.slice(0, 8)
    )
    run_label = st.sidebar.selectbox("Run", run_options["label"].tolist())
    run_id = run_options.loc[run_options["label"] == run_label, "run_id"].iloc[0]
    run = run_options.loc[run_options["run_id"] == run_id].iloc[0]

    st.caption(
        f"{run['description']} | {run['hub']} | "
        f"rows={run['n_selected_analog_rows']} | candidates={run['n_candidates']} | "
        f"run_id={run_id}"
    )

    picks = query(f"""
        select *
        from read_parquet('{PICKS}')
        where run_id = ?
        order by coalesce(hour_ending, 0), rank
    """, (run_id,))

    if len(picks) == 0:
        st.warning("This run selected no analogs.")
        return

    if str(run["match_unit"]) == "hour":
        hour = st.sidebar.selectbox(
            "Hour ending",
            sorted(picks["hour_ending"].dropna().astype(int).unique().tolist()),
        )
        visible_picks = picks[picks["hour_ending"].astype(int) == int(hour)]
    else:
        hour = None
        visible_picks = picks

    show_selected_analogs(visible_picks, match_unit=str(run["match_unit"]))
    show_top_analog_why(visible_picks)
    show_hourly_contributions(run_id, hour)
    show_feature_trace(run_id, visible_picks, hour)
    show_feature_price_correlations(run_id)


def show_selected_analogs(picks: pd.DataFrame, match_unit: str) -> None:
    st.subheader("Selected Analogs")
    display_cols = [
        "hour_ending",
        "rank",
        "analog_date",
        "distance",
        "weight_pct",
        "top_distance_group",
    ]
    display_cols += [c for c in picks.columns if c.startswith("distance_")]
    st.dataframe(
        picks[[c for c in display_cols if c in picks.columns]],
        use_container_width=True,
        hide_index=True,
    )

    fig = px.bar(
        picks.sort_values("weight_pct", ascending=True),
        x="weight_pct",
        y="analog_date",
        color="top_distance_group" if "top_distance_group" in picks.columns else None,
        orientation="h",
        hover_data=[c for c in ["hour_ending", "rank", "distance"] if c in picks.columns],
        labels={
            "analog_date": "Analog date",
            "weight_pct": "Forecast weight",
            "top_distance_group": "Largest distance group",
        },
        title="Analog Weight" if match_unit == "day" else "Analog Weight for Selected Hour",
    )
    fig.update_xaxes(tickformat=".1%")
    st.plotly_chart(fig, use_container_width=True)


def show_top_analog_why(picks: pd.DataFrame) -> None:
    distance_cols = [c for c in picks.columns if c.startswith("distance_")]
    if len(picks) == 0 or not distance_cols:
        return

    top = picks.sort_values("rank").iloc[0]
    group_distances = pd.DataFrame({
        "group": [c.replace("distance_", "") for c in distance_cols],
        "distance": [top[c] for c in distance_cols],
    }).dropna()

    st.subheader(f"Why Top Analog {top['analog_date']} Was Picked")
    fig = px.bar(
        group_distances.sort_values("distance", ascending=False),
        x="group",
        y="distance",
        labels={"group": "Feature group", "distance": "Group distance"},
        title="Top Analog Distance by Feature Group",
    )
    st.plotly_chart(fig, use_container_width=True)


def show_hourly_contributions(run_id: str, hour: int | None) -> None:
    contrib = query(f"""
        select *
        from read_parquet('{CONTRIB}')
        where run_id = ?
        order by hour_ending, rank
    """, (run_id,))
    if len(contrib) == 0:
        return

    st.subheader("Hourly Forecast Contribution")
    plot_contrib = contrib
    if hour is not None:
        plot_contrib = contrib[contrib["hour_ending"].astype(int) == int(hour)]

    heatmap = plot_contrib.pivot_table(
        index="analog_date",
        columns="hour_ending",
        values="lmp_contribution",
        aggfunc="sum",
    )
    fig = px.imshow(
        heatmap,
        aspect="auto",
        labels={
            "x": "Hour ending",
            "y": "Analog date",
            "color": "Weighted LMP contribution",
        },
        title="Analog Contribution by Hour",
    )
    st.plotly_chart(fig, use_container_width=True)

    hourly = contrib.groupby("hour_ending", as_index=False)["lmp_contribution"].sum()
    fig = px.line(
        hourly,
        x="hour_ending",
        y="lmp_contribution",
        markers=True,
        labels={
            "hour_ending": "Hour ending",
            "lmp_contribution": "Point forecast",
        },
        title="Forecast Rebuilt from Analog Contributions",
    )
    st.plotly_chart(fig, use_container_width=True)


def show_feature_trace(run_id: str, picks: pd.DataFrame, hour: int | None) -> None:
    trace = query(f"""
        select *
        from read_parquet('{TRACE}')
        where run_id = ?
        order by coalesce(hour_ending, 0), rank, "group", feature
    """, (run_id,))
    if len(trace) == 0:
        return

    if hour is not None:
        trace = trace[trace["hour_ending"].astype(int) == int(hour)].copy()

    st.subheader("Feature-Level Distance Trace")
    selected_analog = st.selectbox("Analog date", picks["analog_date"].astype(str).tolist())
    trace_one = trace[trace["analog_date"].astype(str) == selected_analog].copy()

    cols = [
        "hour_ending",
        "rank",
        "analog_date",
        "group",
        "feature",
        "target_value",
        "candidate_value",
        "z_delta",
        "abs_z_delta",
        "feature_distance_contribution",
        "group_distance",
        "weighted_group_distance",
    ]
    st.dataframe(
        trace_one[[c for c in cols if c in trace_one.columns]],
        use_container_width=True,
        hide_index=True,
    )

    if "feature_distance_contribution" in trace_one.columns:
        top = trace_one.nlargest(30, "feature_distance_contribution")
        fig = px.bar(
            top.sort_values("feature_distance_contribution", ascending=True),
            x="feature_distance_contribution",
            y="feature",
            color="group",
            orientation="h",
            hover_data=["z_delta", "abs_z_delta", "group_distance"],
            labels={
                "feature_distance_contribution": "Weighted squared z-gap contribution",
                "feature": "Feature",
                "group": "Group",
            },
            title="Largest Feature Gaps for Selected Analog",
        )
        st.plotly_chart(fig, use_container_width=True)


def show_feature_price_correlations(run_id: str) -> None:
    corr = query(f"""
        select *
        from read_parquet('{CORR}')
        where run_id = ?
        order by "group", feature, hour_ending
    """, (run_id,))
    if len(corr) == 0:
        return

    st.subheader("Feature to Price Correlation")
    corr_method = st.radio(
        "Correlation",
        ["spearman_corr", "pearson_corr"],
        horizontal=True,
    )
    feature_options = corr["feature"].drop_duplicates().tolist()
    feature = st.selectbox("Feature", feature_options)

    selected = corr[corr["feature"] == feature]
    fig = px.bar(
        selected,
        x="hour_ending",
        y=corr_method,
        labels={
            "hour_ending": "Hour ending",
            corr_method: corr_method.replace("_", " "),
        },
        title=f"{feature} vs Hourly DA LMP",
    )
    fig.update_yaxes(range=[-1, 1])
    st.plotly_chart(fig, use_container_width=True)

    matrix = corr.pivot_table(
        index="feature",
        columns="hour_ending",
        values=corr_method,
        aggfunc="first",
    )
    fig = px.imshow(
        matrix,
        aspect="auto",
        color_continuous_scale="RdBu_r",
        zmin=-1,
        zmax=1,
        labels={"x": "Hour ending", "y": "Feature", "color": "Correlation"},
        title="Feature/LMP Correlation Heatmap",
    )
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
