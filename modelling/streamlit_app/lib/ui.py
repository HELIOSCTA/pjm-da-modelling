"""Shared UI helpers — wide-format hourly tables, on-peak shading, and the
linked feature-date / target-date pair that syncs across Data, Candidates,
and Run via st.session_state.target_date.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st


CANONICAL_TARGET_KEY = "target_date"


def init_target_date_state(default_offset_days: int = 1) -> None:
    """Ensure the canonical cross-page target_date is initialized.

    Pages with date inputs should call this once before rendering widgets.
    Run binds its date_input directly to ``key="target_date"``; Data and
    Candidates use :func:`linked_date_pair` instead.
    """
    if CANONICAL_TARGET_KEY not in st.session_state:
        st.session_state[CANONICAL_TARGET_KEY] = (
            date.today() + timedelta(days=default_offset_days)
        )


def linked_date_pair(
    *,
    container=None,
    forecast_label: str = "Forecast date",
    target_label: str = "Target date",
    forecast_help: str | None = "Editable. Target date is automatically Forecast date + 1.",
    target_help: str | None = "Auto-computed: Forecast date + 1. Read-only.",
    key_prefix: str = "_linked_date",
) -> tuple[date, date]:
    """Render Forecast date (editable) and Target date (read-only = +1) in ``container``.

    Forecast date is the user-editable input. Target date is disabled and
    always shows Forecast date + 1. Both stay in sync with the cross-page
    canonical ``st.session_state.target_date`` so changes on Run propagate
    to Data/Candidates and vice versa.

    Returns ``(forecast_date, target_date)``.
    """
    init_target_date_state()
    container = container or st.sidebar

    date_key = f"{key_prefix}_d"
    target_key = f"{key_prefix}_t"
    canonical: date = st.session_state[CANONICAL_TARGET_KEY]

    # Force-sync widget state from canonical (handles cross-page navigation
    # where another page changed target_date while we were elsewhere).
    if st.session_state.get(date_key) != canonical - timedelta(days=1):
        st.session_state[date_key] = canonical - timedelta(days=1)
    if st.session_state.get(target_key) != canonical:
        st.session_state[target_key] = canonical

    def _on_date_change() -> None:
        new_target = st.session_state[date_key] + timedelta(days=1)
        st.session_state[CANONICAL_TARGET_KEY] = new_target
        st.session_state[target_key] = new_target

    container.date_input(
        forecast_label, key=date_key, on_change=_on_date_change, help=forecast_help,
    )
    container.date_input(
        target_label, key=target_key, disabled=True, help=target_help,
    )

    return st.session_state[date_key], st.session_state[CANONICAL_TARGET_KEY]

OFF_PEAK_HOURS = list(range(1, 8)) + [24]
ON_PEAK_HOURS = list(range(8, 24))
ALL_HOURS = list(range(1, 25))

NUMERIC_COLS = [f"HE{h}" for h in ALL_HOURS] + ["OnPeak", "OffPeak", "Flat"]
ON_PEAK_HIGHLIGHT_COLS = [f"HE{h}" for h in ON_PEAK_HOURS] + ["OnPeak"]
ON_PEAK_BG = "rgba(255, 215, 0, 0.18)"


def wide_summary_row(
    df: pd.DataFrame,
    *,
    source: str,
    region: str,
    target_date: date,
    value_col: str,
) -> pd.DataFrame:
    """One-row wide-format table: Source · Region · Date · HE1..HE24 · OnPeak · OffPeak · Flat."""
    by_he = (
        df.dropna(subset=["hour_ending"])
        .assign(hour_ending=lambda d: d["hour_ending"].astype(int))
        .groupby("hour_ending")[value_col]
        .mean()
        .reindex(ALL_HOURS)
    )

    def _avg(hours: list[int]) -> float:
        return by_he.reindex(hours).mean()

    row: dict[str, object] = {
        "Source": source,
        "Region": region,
        "Date": str(target_date),
    }
    for h in ALL_HOURS:
        row[f"HE{h}"] = by_he.get(h)
    row["OnPeak"] = _avg(ON_PEAK_HOURS)
    row["OffPeak"] = _avg(OFF_PEAK_HOURS)
    row["Flat"] = _avg(ALL_HOURS)
    return pd.DataFrame([row])


def styled_summary(df: pd.DataFrame, *, decimals: int):
    """Highlight on-peak columns + format numerics with thousands separators."""
    fmt = f"{{:,.{decimals}f}}"
    return (
        df.style
        .set_properties(
            subset=ON_PEAK_HIGHLIGHT_COLS,
            **{"background-color": ON_PEAK_BG},
        )
        .format(fmt, subset=NUMERIC_COLS, na_rep="—")
    )


def shade_onpeak(fig):
    """Shade HE 8-23 and tighten the x-axis so hour positions match table cells."""
    fig.add_vrect(
        x0=7.5, x1=23.5,
        fillcolor="gold",
        opacity=0.18,
        layer="below",
        line_width=0,
    )
    fig.update_xaxes(
        range=[0.5, 24.5],
        tick0=1,
        dtick=2,
    )
    fig.update_layout(margin=dict(l=60, r=20, t=40, b=40))
    return fig
