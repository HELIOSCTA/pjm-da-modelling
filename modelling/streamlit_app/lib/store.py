"""DuckDB-backed reader for the like_day_model_knn analog Parquet store."""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

_DEFAULT_STORE_DIR = (
    Path(__file__).resolve().parents[2]
    / "da_models"
    / "like_day_model_knn"
    / "output"
    / "analog_store"
)


def store_dir() -> Path:
    return Path(os.getenv("KNN_ANALOG_STORE_DIR", str(_DEFAULT_STORE_DIR)))


def store_has_data() -> bool:
    return any((store_dir() / "runs").glob("*.parquet"))


def _glob(table: str) -> str:
    return store_dir().joinpath(table, "*.parquet").as_posix()


@st.cache_data(show_spinner=False)
def _query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with duckdb.connect() as con:
        return con.execute(sql, params).df()


def clear_cache() -> None:
    _query.clear()


def load_runs() -> pd.DataFrame:
    return _query(f"""
        select *
        from read_parquet('{_glob("runs")}', union_by_name=true)
        order by target_date desc, model_name, created_at_utc desc
    """)


def _load_filtered(table: str, run_ids: list[str], order_by: str) -> pd.DataFrame:
    if not run_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(run_ids))
    return _query(
        f"""
        select *
        from read_parquet('{_glob(table)}', union_by_name=true)
        where run_id in ({placeholders})
        order by {order_by}
        """,
        tuple(run_ids),
    )


def load_picks(run_ids: list[str]) -> pd.DataFrame:
    return _load_filtered(
        "analog_picks",
        run_ids,
        order_by="run_id, coalesce(hour_ending, 0), rank",
    )


def load_contributions(run_ids: list[str]) -> pd.DataFrame:
    return _load_filtered(
        "hourly_contributions",
        run_ids,
        order_by="run_id, hour_ending, rank",
    )


def load_feature_trace(run_ids: list[str]) -> pd.DataFrame:
    return _load_filtered(
        "analog_feature_trace",
        run_ids,
        order_by='run_id, coalesce(hour_ending, 0), rank, "group", feature',
    )


def load_correlations(run_ids: list[str]) -> pd.DataFrame:
    return _load_filtered(
        "feature_price_correlations",
        run_ids,
        order_by='run_id, "group", feature, hour_ending',
    )
