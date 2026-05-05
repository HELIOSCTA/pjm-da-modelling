"""Long-format pool + query assembly for like_day_model_knn_sunny.

Sunny's variant uses one row per ``(date, hour_ending)`` (vs. the
sibling's wide ``(date, lmp_h1..lmp_h24)`` shape). Hourly domains are
outer-joined on ``(date, hour_ending)``; daily-broadcast domains are
outer-joined on ``date`` so pandas naturally fans the daily value across
all 24 HE rows. Calendar features and a single ``lmp`` label are merged
last.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.common.data import loader
from da_models.common.data.lmp_pool import build_lmp_labels, load_lmp_da
from da_models.like_day_model_knn_sunny import calendar as _calendar
from da_models.like_day_model_knn_sunny import configs
from da_models.like_day_model_knn_sunny.domains import (
    DAILY_BROADCAST_DOMAINS,
    DOMAIN_REGISTRY,
    all_feature_cols,
)

logger = logging.getLogger(__name__)


CALENDAR_COLS: list[str] = [
    "day_of_week_number",
    "is_nerc_holiday",
    "is_weekend",
    "dow_sin",
    "dow_cos",
]


def _to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s).dt.date


def _attach_calendar(
    pool: pd.DataFrame,
    cache_dir: Path | None,
) -> pd.DataFrame:
    dates = pd.Series(pool["date"].drop_duplicates().tolist(), name="date").reset_index(
        drop=True
    )
    if len(dates) == 0:
        for c in CALENDAR_COLS:
            pool[c] = pd.Series(dtype=float)
        return pool

    dates_meta = _calendar.load_pjm_dates_daily(cache_dir=cache_dir)
    holiday_lookup: dict = {}
    if (
        dates_meta is not None
        and len(dates_meta) > 0
        and "is_nerc_holiday" in dates_meta.columns
    ):
        meta = dates_meta[["date", "is_nerc_holiday"]].copy()
        meta["date"] = _to_date(meta["date"])
        holiday_lookup = dict(
            zip(
                meta["date"].tolist(),
                meta["is_nerc_holiday"].fillna(0).astype(int).tolist(),
            )
        )

    rows: list[dict] = []
    for d in dates.tolist():
        is_hol = bool(holiday_lookup.get(d, 0))
        row = {"date": d}
        row.update(_calendar.compute_sunny_calendar_row(d, is_nerc_holiday=is_hol))
        rows.append(row)
    cal_df = pd.DataFrame(rows)
    return pool.merge(cal_df, on="date", how="left")


def _build_lmp_long(
    cache_dir: Path | None,
    hub: str,
    label_source: str,
) -> pd.DataFrame:
    if label_source == "hub_lmp":
        df_lmp = load_lmp_da(cache_dir=cache_dir)
        labels_wide = build_lmp_labels(df_lmp, hub)
        if len(labels_wide) == 0:
            return pd.DataFrame(columns=["date", "hour_ending", "lmp"])
        long = labels_wide.melt(
            id_vars=["date"],
            var_name="hour_ending",
            value_name="lmp",
        )
        long["hour_ending"] = (
            long["hour_ending"].str.replace("lmp_h", "", regex=False).astype(int)
        )
        long["date"] = _to_date(long["date"])
        return long[["date", "hour_ending", "lmp"]].dropna(
            subset=["date", "hour_ending"]
        )

    if label_source == "system_energy":
        try:
            df_sep = loader.load_lmp_system_energy_da(cache_dir=cache_dir)
        except KeyError as exc:
            raise KeyError(
                "label_source='system_energy' requires lmp_system_energy_price in the LMP frame; "
                "cache may be stale - re-ingest pjm_lmps_hourly.parquet."
            ) from exc
        if (
            df_sep is None
            or len(df_sep) == 0
            or "lmp_system_energy_price" not in df_sep.columns
        ):
            raise KeyError(
                "label_source='system_energy' requires lmp_system_energy_price in the LMP frame; "
                "cache may be stale - re-ingest pjm_lmps_hourly.parquet."
            )
        sep = df_sep[df_sep["region"].astype(str) == hub].copy()
        if len(sep) == 0:
            sep = df_sep.copy()
        sep["date"] = _to_date(sep["date"])
        sep["hour_ending"] = pd.to_numeric(sep["hour_ending"], errors="coerce")
        sep = sep.dropna(subset=["date", "hour_ending"]).copy()
        sep["hour_ending"] = sep["hour_ending"].astype(int)
        sep = sep.rename(columns={"lmp_system_energy_price": "lmp"})
        sep = sep.drop_duplicates(subset=["date", "hour_ending"], keep="first")
        return sep[["date", "hour_ending", "lmp"]]

    raise ValueError(f"Unknown label_source: {label_source!r}")


def build_pool_from_spec(
    spec: configs.ModelSpec,
    hub: str = configs.HUB,
    label_source: str = configs.LABEL_SOURCE,
    cache_dir: Path | None = configs.CACHE_DIR,
) -> pd.DataFrame:
    if not spec.domains:
        raise ValueError(f"Spec '{spec.name}' has no domains.")

    feat: pd.DataFrame | None = None
    for name in spec.domains:
        domain = DOMAIN_REGISTRY[name]
        df = domain.pool_builder(cache_dir)
        if df is None or len(df) == 0:
            continue
        df = df.copy()
        df["date"] = _to_date(df["date"])
        if name in DAILY_BROADCAST_DOMAINS:
            on = ["date"]
        else:
            df["hour_ending"] = pd.to_numeric(
                df["hour_ending"], errors="coerce"
            ).astype("Int64")
            df = df.dropna(subset=["date", "hour_ending"]).copy()
            df["hour_ending"] = df["hour_ending"].astype(int)
            on = ["date", "hour_ending"]
        if feat is None:
            feat = df
        else:
            if "hour_ending" in feat.columns and on == ["date"]:
                feat = feat.merge(df, on=["date"], how="outer")
            elif "hour_ending" not in feat.columns and on == ["date", "hour_ending"]:
                feat = df.merge(feat, on=["date"], how="outer")
            else:
                feat = feat.merge(df, on=on, how="outer")

    if feat is None or len(feat) == 0:
        feat = pd.DataFrame(
            columns=["date", "hour_ending"] + all_feature_cols(spec.domains)
        )

    if "hour_ending" not in feat.columns:
        hours = pd.DataFrame(
            {
                "hour_ending": list(configs.HOURS)
                if hasattr(configs, "HOURS")
                else list(range(1, 25))
            }
        )
        feat["__key"] = 1
        hours["__key"] = 1
        feat = feat.merge(hours, on="__key").drop(columns=["__key"])

    feat = _attach_calendar(feat, cache_dir)

    lmp_long = _build_lmp_long(cache_dir=cache_dir, hub=hub, label_source=label_source)
    pool = feat.merge(lmp_long, on=["date", "hour_ending"], how="left")

    feature_cols = all_feature_cols(spec.domains)
    keep_cols = ["date", "hour_ending"] + feature_cols + CALENDAR_COLS + ["lmp"]
    for c in keep_cols:
        if c not in pool.columns:
            pool[c] = np.nan
    pool = pool[keep_cols]
    pool = pool.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    n_rows = len(pool)
    n_dates = pool["date"].nunique()
    n_lmp = int(pool["lmp"].notna().sum())
    fill = float(n_lmp / n_rows) if n_rows else 0.0
    logger.info(
        "%s pool (long): %d rows over %d dates; lmp fill %d/%d (%.2f%%); domains=%s",
        spec.name,
        n_rows,
        n_dates,
        n_lmp,
        n_rows,
        fill * 100.0,
        spec.domains,
    )
    return pool


def build_query_row_from_spec(
    spec: configs.ModelSpec,
    target_date: date,
    cache_dir: Path | None = configs.CACHE_DIR,
) -> pd.DataFrame:
    if not spec.domains:
        raise ValueError(f"Spec '{spec.name}' has no domains.")

    HOURS = list(range(1, 25))
    base = pd.DataFrame({"date": [target_date] * 24, "hour_ending": HOURS})

    for name in spec.domains:
        domain = DOMAIN_REGISTRY[name]
        df = domain.query_builder(target_date, cache_dir)
        if df is None or len(df) == 0:
            for c in domain.feature_cols:
                if c not in base.columns:
                    base[c] = np.nan
            continue
        df = df.copy()
        df["date"] = _to_date(df["date"])
        if name in DAILY_BROADCAST_DOMAINS:
            df = df[df["date"] == target_date]
            if len(df) == 0:
                for c in domain.feature_cols:
                    if c not in base.columns:
                        base[c] = np.nan
                continue
            for c in domain.feature_cols:
                base[c] = df.iloc[0].get(c, np.nan)
        else:
            df["hour_ending"] = pd.to_numeric(
                df["hour_ending"], errors="coerce"
            ).astype("Int64")
            df = df.dropna(subset=["date", "hour_ending"]).copy()
            df["hour_ending"] = df["hour_ending"].astype(int)
            df = df[df["date"] == target_date]
            keep = ["date", "hour_ending"] + [
                c for c in domain.feature_cols if c in df.columns
            ]
            base = base.merge(df[keep], on=["date", "hour_ending"], how="left")
            for c in domain.feature_cols:
                if c not in base.columns:
                    base[c] = np.nan

    dates_meta = _calendar.load_pjm_dates_daily(cache_dir=cache_dir)
    is_hol = False
    if dates_meta is not None and len(dates_meta) > 0:
        sub = dates_meta[dates_meta["date"] == target_date]
        if len(sub) > 0 and "is_nerc_holiday" in sub.columns:
            is_hol = bool(sub.iloc[0].get("is_nerc_holiday", 0))
    cal = _calendar.compute_sunny_calendar_row(target_date, is_nerc_holiday=is_hol)
    for k, v in cal.items():
        base[k] = v

    feature_cols = all_feature_cols(spec.domains)
    keep_cols = ["date", "hour_ending"] + feature_cols + CALENDAR_COLS
    for c in keep_cols:
        if c not in base.columns:
            base[c] = np.nan
    out = base[keep_cols].sort_values("hour_ending").reset_index(drop=True)
    return out
