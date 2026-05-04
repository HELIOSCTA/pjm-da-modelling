"""DA LMP loader — reads pjm_lmps_hourly parquet, filters market='da'.

Cache-only. The Prefect scheduler refreshes the parquet; this loader
never falls back to Postgres.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from backend.utils import paths


def load_da_lmps(target_date: date, lookback_days: int = 14) -> pd.DataFrame:
    """Return DA LMPs for `target_date` plus the prior `lookback_days - 1` days.

    Raises ValueError if `target_date` is not present in the parquet — the
    DA market may not have cleared yet for that delivery day.

    Output columns: date, hour_ending, hub, lmp_total,
    lmp_system_energy_price, lmp_congestion_price, lmp_marginal_loss_price.
    """
    raw = pd.read_parquet(paths.parquet("lmps_hourly"))
    df = raw[raw["market"] == "da"].copy()

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")
    df["hour_ending"] = df["hour_ending"].replace(0, 24)
    df = df.dropna(subset=["date", "hour_ending"])
    df["hour_ending"] = df["hour_ending"].astype(int)
    df = df[df["hour_ending"].between(1, 24)]

    if not (df["date"] == target_date).any():
        latest = df["date"].max()
        raise ValueError(
            f"No DA LMP data for {target_date} in {paths.parquet('lmps_hourly').name}. "
            f"Latest cleared day in parquet: {latest}. "
            "Has PJM cleared DA yet (~13:00 EPT)?"
        )

    cutoff = target_date - timedelta(days=lookback_days - 1)
    df = df[(df["date"] >= cutoff) & (df["date"] <= target_date)]

    keep = [
        "date", "hour_ending", "hub",
        "lmp_total", "lmp_system_energy_price",
        "lmp_congestion_price", "lmp_marginal_loss_price",
    ]
    return df[keep].sort_values(["hub", "date", "hour_ending"]).reset_index(drop=True)
