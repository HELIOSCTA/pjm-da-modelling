"""Shared parquet loading + pool/query assembly for the three model builders.

After the T4 wide→long cutover, ``build_pool_from_spec`` returns a long
DataFrame (one row per (date, hour_ending), 1 scalar value per feature)
and ``build_query_row_from_spec`` returns a 24-row DataFrame for the
target date. Domain pool builders still produce wide format internally
(``{stem}_h1..{stem}_h24`` cols) — the melt step in
``_melt_pool_to_long`` converts to long with sunny-compatible col names
(``load_mw_at_hour``, ``temp_at_hour``, etc.). Spec ``feature_groups``
reference the long col names.

The per-model builder modules are thin wrappers around
``build_pool_from_spec`` / ``build_query_row_from_spec``; the spec's
``domains`` field drives which features are pulled in and joined.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from backend.modelling.da_models.common.data import loader
from backend.modelling.da_models.common.data.loader import _resolve_cache_dir
from backend.modelling.da_models.common.data.lmp_pool import (
    LMP_HOUR_COLUMNS,
    build_lmp_labels,
    load_lmp_da,
)
from backend.modelling.da_models.like_day_model_knn import configs
from backend.modelling.da_models.like_day_model_knn.domains import (
    DOMAIN_REGISTRY,
    HOURLY_STEM_TO_LONG_COL,
    all_feature_cols,
)

logger = logging.getLogger(__name__)


# Pattern to detect wide hourly cols of the form ``{stem}_h{N}`` where
# N is in 1..24. Used by ``_melt_pool_to_long`` to identify which cols
# need pivoting from wide to long.
_HOURLY_COL_PATTERN = re.compile(r"^(.+)_h(\d+)$")


def resolved_load_forecast_paths(cache_dir: Path | None) -> list[Path]:
    """Absolute paths of the load-forecast parquets that exist on disk."""
    resolved = _resolve_cache_dir(cache_dir)
    return [
        resolved / name
        for name in configs.LOAD_FORECAST_PARQUETS
        if (resolved / name).exists()
    ]


def load_pjm_load_forecast(cache_dir: Path | None) -> pd.DataFrame:
    """Load PJM load-forecast features from the historical-backfill parquet."""
    paths = resolved_load_forecast_paths(cache_dir)
    if not paths:
        logger.warning(
            "No load-forecast parquets found at %s (looked for %s) - "
            "falling back to default loader search",
            _resolve_cache_dir(cache_dir),
            configs.LOAD_FORECAST_PARQUETS,
        )
        return loader.load_load_forecast(cache_dir=cache_dir)

    parts: list[pd.DataFrame] = []
    for p in paths:
        parts.append(loader.load_load_forecast(path=p))
        logger.info("Loaded PJM load forecast: %s", p.name)

    df = pd.concat(parts, ignore_index=True)
    df = df.drop_duplicates(subset=["date", "hour_ending", "region"], keep="first")
    df = df.sort_values(["region", "date", "hour_ending"]).reset_index(drop=True)
    return df


def filter_to_region(df: pd.DataFrame, region: str) -> pd.DataFrame:
    """Restrict a hourly load forecast frame to a specific PJM region (e.g. RTO).

    Assumes canonical dtypes from ``loader._normalize_load_forecast``.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()
    if "region" in df.columns:
        return df[df["region"] == region].copy()
    return df.copy()


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Add NaN columns for any names missing from the frame, preserving order."""
    out = df.copy()
    missing = [c for c in columns if c not in out.columns]
    if missing:
        nan_df = pd.DataFrame({c: np.nan for c in missing}, index=out.index)
        out = pd.concat([out, nan_df], axis=1)
    return out


def load_dates_daily(cache_dir: Path | None) -> pd.DataFrame:
    """Calendar metadata frame from ``pjm_dates_daily.parquet``.

    Thin wrapper around ``calendar.load_pjm_dates_daily`` so per-model
    forecast/single_day modules don't need to import the calendar module
    directly; they already import ``_shared``.
    """
    from backend.modelling.da_models.like_day_model_knn import calendar as _calendar

    return _calendar.load_pjm_dates_daily(cache_dir=cache_dir)


def load_hourly_rto(cache_dir: Path | None) -> pd.DataFrame:
    """PJM RTO hourly load forecast, region-filtered.

    Types are already canonical from ``loader._normalize_load_forecast`` -
    no re-coercion needed here.
    """
    df = load_pjm_load_forecast(cache_dir=cache_dir)
    if "region" in df.columns:
        df = df[df["region"] == configs.LOAD_REGION].copy()
    return df


# ── Spec-driven pool/query assembly ─────────────────────────────────────


def _build_system_energy_labels(df_sep: pd.DataFrame, hub: str) -> pd.DataFrame:
    """Wide-format LMP labels from PJM DA system energy price data.

    Parallel to ``build_lmp_labels`` but sourced from
    ``loader.load_lmp_system_energy_da``. SEP is system-wide so all hubs
    return the same series for a given (date, HE); the hub filter
    deduplicates the per-hub rows in the parquet rather than narrowing
    geographically. Output columns: ``date``, ``lmp_h1..lmp_h24``.
    """
    if df_sep is None or len(df_sep) == 0:
        return pd.DataFrame(columns=["date"] + LMP_HOUR_COLUMNS)
    df = df_sep[df_sep["region"].astype(str) == hub].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype(
        "Int64"
    )
    df["lmp_system_energy_price"] = pd.to_numeric(
        df["lmp_system_energy_price"], errors="coerce"
    )
    df = df.dropna(subset=["date", "hour_ending", "lmp_system_energy_price"])
    if len(df) == 0:
        return pd.DataFrame(columns=["date"] + LMP_HOUR_COLUMNS)
    df["hour_ending"] = df["hour_ending"].astype(int)
    pivot = df.pivot_table(
        index="date",
        columns="hour_ending",
        values="lmp_system_energy_price",
        aggfunc="mean",
    ).reindex(columns=range(1, 25))
    pivot = pivot.rename(columns={h: f"lmp_h{h}" for h in range(1, 25)})
    return pivot.reset_index()


def _melt_pool_to_long(wide_pool: pd.DataFrame) -> pd.DataFrame:
    """Convert wide pool (24 hourly cols per stem) to long ((date, HE) rows).

    For each ``{stem}_h{N}`` family of cols (N in 1..24), the 24 cols
    are unpivoted into a single scalar col named per
    ``HOURLY_STEM_TO_LONG_COL`` (e.g. ``load_h*`` → ``load_mw_at_hour``,
    ``lmp_h*`` → ``lmp``). Stems not in the mapping are skipped with a
    warning. Daily/broadcast cols (no ``_h{N}`` suffix) are joined on
    ``date``, replicated across all 24 HE rows of each date.

    Output schema: ``date``, ``hour_ending``, plus one scalar col per
    melted stem and the broadcast cols.
    """
    if wide_pool is None or len(wide_pool) == 0:
        return pd.DataFrame(columns=["date", "hour_ending"])
    if "date" not in wide_pool.columns:
        raise ValueError("_melt_pool_to_long: wide_pool missing 'date' column")

    # Group cols by stem.
    stems: dict[str, list[tuple[str, int]]] = {}  # stem -> [(col, he), ...]
    broadcast_cols: list[str] = []
    for col in wide_pool.columns:
        if col == "date":
            continue
        m = _HOURLY_COL_PATTERN.match(col)
        if m and 1 <= int(m.group(2)) <= 24:
            stem = m.group(1)
            he = int(m.group(2))
            stems.setdefault(stem, []).append((col, he))
        else:
            broadcast_cols.append(col)

    long_parts: list[pd.DataFrame] = []
    for stem, col_he_pairs in stems.items():
        long_col = HOURLY_STEM_TO_LONG_COL.get(stem)
        if long_col is None:
            logger.warning(
                "_melt_pool_to_long: unknown stem %r (cols=%s) — skipping",
                stem,
                [c for c, _ in col_he_pairs],
            )
            continue
        # Sort by HE so the melt produces a stable order (1..24).
        col_he_pairs.sort(key=lambda t: t[1])
        cols = [c for c, _ in col_he_pairs]
        sub = wide_pool[["date"] + cols].melt(
            id_vars="date",
            value_vars=cols,
            var_name="_he_col",
            value_name=long_col,
        )
        # Extract HE number from col name.
        sub["hour_ending"] = sub["_he_col"].apply(
            lambda c: int(_HOURLY_COL_PATTERN.match(c).group(2))
        )
        sub = sub[["date", "hour_ending", long_col]]
        long_parts.append(sub)

    if not long_parts:
        # No hourly stems found — return an empty long frame with broadcast.
        skel = wide_pool[["date"]].copy()
        skel["_he"] = [list(range(1, 25))] * len(skel)
        skel = skel.explode("_he").rename(columns={"_he": "hour_ending"})
        skel["hour_ending"] = skel["hour_ending"].astype(int)
        long = skel
    else:
        long = long_parts[0]
        for part in long_parts[1:]:
            long = long.merge(part, on=["date", "hour_ending"], how="outer")

    # Broadcast cols — joined on date, replicated across HE rows.
    if broadcast_cols:
        long = long.merge(wide_pool[["date"] + broadcast_cols], on="date", how="left")

    return long.sort_values(["date", "hour_ending"]).reset_index(drop=True)


def build_pool_from_spec(
    spec: configs.ModelSpec,
    hub: str = configs.HUB,
    cache_dir: Path | None = configs.CACHE_DIR,
    label_source: str = configs.LABEL_SOURCE,
) -> pd.DataFrame:
    """Long-format pool: one row per (date, hour_ending) for every delivery
    date in history, carrying scalar feature values for that HE plus the
    LMP label and broadcast features (outage/gas/calendar).

    Domains' wide pool builders are pulled per ``spec.domains``,
    outer-joined on ``date``, then ``_melt_pool_to_long`` pivots the
    ``{stem}_h1..h24`` cols into single scalar long cols matching
    sunny's naming (``load_mw_at_hour``, ``temp_at_hour``, etc.).

    ``label_source``:
      - ``"hub_lmp"`` (default): total DA LMP at ``hub``.
      - ``"system_energy"``: PJM RTO DA System Energy Price (LMP minus
        congestion + loss). SEP is system-wide so the hub filter only
        deduplicates per-hub rows.
    """
    if not spec.domains:
        raise ValueError(f"Spec '{spec.name}' has no domains.")

    feat: pd.DataFrame | None = None
    for name in spec.domains:
        domain = DOMAIN_REGISTRY[name]
        df = domain.pool_builder(cache_dir)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        if feat is None:
            feat = df
        else:
            feat = feat.merge(df, on="date", how="outer")

    if label_source == "hub_lmp":
        df_lmp_da = load_lmp_da(cache_dir=cache_dir)
        df_labels = build_lmp_labels(df_lmp_da, hub)
    elif label_source == "system_energy":
        df_sep = loader.load_lmp_system_energy_da(cache_dir=cache_dir)
        df_labels = _build_system_energy_labels(df_sep, hub)
    else:
        raise ValueError(
            f"Unknown label_source={label_source!r}; "
            "expected 'hub_lmp' or 'system_energy'."
        )
    wide_pool = (
        feat.merge(df_labels, on="date", how="left")
        .sort_values("date")
        .reset_index(drop=True)
    )

    long_pool = _melt_pool_to_long(wide_pool)

    # Coverage telemetry.
    n_dates = long_pool["date"].nunique() if "date" in long_pool.columns else 0
    n_rows = len(long_pool)
    feature_long_cols = [
        c for c in long_pool.columns if c not in ("date", "hour_ending")
    ]
    n_feature_cols = len(feature_long_cols)
    has_lmp = "lmp" in long_pool.columns
    n_with_lmp = int(long_pool["lmp"].notna().sum()) if has_lmp else 0
    logger.info(
        "%s pool (long): %d rows / %d unique dates x %d feature cols "
        "(%d rows w/ lmp) — domains=%s, label_source=%s",
        spec.name,
        n_rows,
        n_dates,
        n_feature_cols,
        n_with_lmp,
        spec.domains,
        label_source,
    )
    return long_pool


def build_query_row_from_spec(
    spec: configs.ModelSpec,
    target_date: date,
    cache_dir: Path | None = configs.CACHE_DIR,
) -> pd.DataFrame:
    """Long-format query: 24-row DataFrame keyed by (date, hour_ending),
    one row per HE, carrying scalar feature values for the target date.

    Each domain's query builder returns a one-row wide frame for the
    target date; the merged wide query is melted to long via
    ``_melt_pool_to_long`` (no LMP since the query is unrealized).
    """
    if not spec.domains:
        raise ValueError(f"Spec '{spec.name}' has no domains.")

    parts: list[pd.DataFrame] = []
    for name in spec.domains:
        domain = DOMAIN_REGISTRY[name]
        df = domain.query_builder(target_date, cache_dir)
        if len(df) == 0:
            empty = {"date": target_date, **{c: np.nan for c in domain.feature_cols}}
            df = pd.DataFrame([empty])
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[df["date"] == target_date]
        if len(df) == 0:
            empty = {"date": target_date, **{c: np.nan for c in domain.feature_cols}}
            df = pd.DataFrame([empty])
        parts.append(df.iloc[[0]].reset_index(drop=True))

    wide = parts[0]
    for p in parts[1:]:
        wide = wide.merge(p, on="date", how="left")

    long = _melt_pool_to_long(wide)
    n_filled = int(
        long.drop(columns=["date", "hour_ending"], errors="ignore")
        .notna()
        .any(axis=1)
        .sum()
    )
    logger.info(
        "%s query for %s: %d/24 HE rows w/ any feature — domains=%s",
        spec.name,
        target_date,
        n_filled,
        spec.domains,
    )
    return long


# ── Legacy compat ──────────────────────────────────────────────────────
#
# Some upstream callers still reference ``all_feature_cols`` (the wide
# convention's catalog of every feature col across enabled domains).
# Keep the import alive so those callers don't ImportError; the symbol
# now returns long-format col names per domain.feature_groups.


_ = all_feature_cols  # re-export for callers that haven't migrated yet
