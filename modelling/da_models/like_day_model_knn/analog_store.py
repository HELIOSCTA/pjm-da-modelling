"""Parquet explainability store for like_day_model_knn backtests."""
from __future__ import annotations

import uuid
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.like_day_model_knn import configs
from da_models.like_day_model_knn.configs import KnnModelConfig, ModelSpec

HOURS = list(range(1, 25))
LMP_COLS = [f"lmp_h{h}" for h in HOURS]
DEFAULT_STORE_DIR = Path(__file__).resolve().parent / "output" / "analog_store"


def write_analog_explainability(
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    pool: pd.DataFrame,
    query: pd.Series,
    analogs: pd.DataFrame,
    output_dir: Path | None = None,
) -> str:
    """Append a single run's analog explainability tables to the Parquet store."""
    run_id = str(uuid.uuid4())
    output_dir = Path(output_dir) if output_dir is not None else DEFAULT_STORE_DIR
    _ensure_store_dirs(output_dir)

    work = _candidate_pool(
        pool=pool,
        target_date=target_date,
        season_window_days=config.season_window_days,
        min_pool_size=config.min_pool_size,
    )

    _write_run_manifest(
        output_dir=output_dir,
        run_id=run_id,
        target_date=target_date,
        config=config,
        spec=spec,
        n_pool=len(pool),
        n_candidates=len(work),
        n_analogs=len(analogs),
    )

    if len(analogs) == 0:
        return run_id

    if spec.match_unit == "hour":
        candidates, feature_trace = _explain_hour_candidates(query, work, spec)
        picks = _build_hour_picks(run_id, target_date, config, spec, analogs, candidates)
        contributions = _build_hour_contributions(run_id, target_date, config, spec, picks)
        selected = set(
            zip(
                picks["hour_ending"].astype(int),
                picks["analog_date"].astype(str),
            ),
        )
        trace = _build_selected_hour_trace(
            run_id=run_id,
            target_date=target_date,
            config=config,
            spec=spec,
            feature_trace=feature_trace,
            selected=selected,
        )
    else:
        candidates, feature_trace = _explain_day_candidates(query, work, spec)
        picks = _build_day_picks(run_id, target_date, config, spec, analogs, candidates)
        contributions = _build_day_contributions(run_id, target_date, config, spec, picks)
        trace = _build_selected_day_trace(
            run_id=run_id,
            target_date=target_date,
            config=config,
            spec=spec,
            feature_trace=feature_trace,
            selected_dates=set(picks["analog_date"].astype(str)),
        )

    correlations = _build_feature_price_correlations(
        run_id=run_id,
        target_date=target_date,
        config=config,
        spec=spec,
        pool=work,
    )

    _write_table(output_dir / "analog_picks" / f"{run_id}.parquet", picks)
    _write_table(output_dir / "analog_feature_trace" / f"{run_id}.parquet", trace)
    _write_table(output_dir / "hourly_contributions" / f"{run_id}.parquet", contributions)
    _write_table(output_dir / "feature_price_correlations" / f"{run_id}.parquet", correlations)
    return run_id


def _ensure_store_dirs(output_dir: Path) -> None:
    for name in (
        "runs",
        "analog_picks",
        "analog_feature_trace",
        "hourly_contributions",
        "feature_price_correlations",
    ):
        (output_dir / name).mkdir(parents=True, exist_ok=True)


def _candidate_pool(
    pool: pd.DataFrame,
    target_date: date,
    season_window_days: int,
    min_pool_size: int,
) -> pd.DataFrame:
    work = pool.copy()
    work = work[pd.to_datetime(work["date"]).dt.date < target_date].copy()
    if len(work) == 0 or season_window_days <= 0:
        return work

    target_doy = pd.Timestamp(target_date).dayofyear
    doys = pd.to_datetime(work["date"]).dt.dayofyear.to_numpy(dtype=float)
    direct = np.abs(doys - float(target_doy))
    keep = np.minimum(direct, 366.0 - direct) <= float(season_window_days)
    candidates = work[keep]
    return candidates.copy() if len(candidates) >= min_pool_size else work


def _write_run_manifest(
    output_dir: Path,
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    n_pool: int,
    n_candidates: int,
    n_analogs: int,
) -> None:
    row = pd.DataFrame([{
        "run_id": run_id,
        "created_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
        "target_date": str(target_date),
        "model_name": spec.name,
        "match_unit": spec.match_unit,
        "description": spec.description,
        "hub": config.hub,
        "schema": config.schema,
        "n_analogs": int(config.n_analogs),
        "season_window_days": int(config.season_window_days),
        "min_pool_size": int(config.min_pool_size),
        "flt_radius": int(spec.flt_radius),
        "n_pool": int(n_pool),
        "n_candidates": int(n_candidates),
        "n_selected_analog_rows": int(n_analogs),
    }])
    _write_table(output_dir / "runs" / f"{run_id}.parquet", row)


def _explain_day_candidates(
    query: pd.Series,
    work: pd.DataFrame,
    spec: ModelSpec,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    n = len(work)
    weighted_sum = np.zeros(n, dtype=float)
    weight_sum = np.zeros(n, dtype=float)
    group_distance_by_name: dict[str, np.ndarray] = {}
    feature_rows: list[dict] = []

    for group, cols in spec.feature_groups.items():
        group_weight = float(spec.feature_group_weights.get(group, 0.0))
        if group_weight <= 0:
            continue
        cols_present = [c for c in cols if c in work.columns and c in query.index]
        if not cols_present:
            continue

        pool_vals = work[cols_present].to_numpy(dtype=float)
        query_vals = query[cols_present].to_numpy(dtype=float)
        means, stds = _zscore_fit(pool_vals)
        pool_z = (pool_vals - means) / stds
        query_z = ((query_vals - means) / stds).reshape(-1)

        group_distances = np.full(n, np.nan, dtype=float)
        for i in range(n):
            diff = query_z - pool_z[i]
            mask = ~np.isnan(diff)
            n_valid = int(mask.sum())
            if n_valid == 0:
                continue

            squared_delta = diff[mask] ** 2
            group_distance = float(np.sqrt(np.sum(squared_delta) / n_valid))
            group_distances[i] = group_distance
            weighted_sum[i] += group_weight * group_distance
            weight_sum[i] += group_weight

            for j in np.where(mask)[0]:
                feature_rows.append({
                    "date": work.iloc[i]["date"],
                    "hour_ending": np.nan,
                    "group": group,
                    "group_weight": group_weight,
                    "feature": cols_present[j],
                    "target_value": query_vals[j],
                    "candidate_value": pool_vals[i, j],
                    "pool_mean": means[j],
                    "pool_std": stds[j],
                    "target_z": query_z[j],
                    "candidate_z": pool_z[i, j],
                    "z_delta": diff[j],
                    "abs_z_delta": abs(diff[j]),
                    "squared_delta": diff[j] ** 2,
                    "n_valid_in_group": n_valid,
                    "group_distance": group_distance,
                    "weighted_group_distance": group_weight * group_distance,
                })

        group_distance_by_name[group] = group_distances

    candidates = _rank_candidates(work, weighted_sum, weight_sum)
    for group, values in group_distance_by_name.items():
        candidates[f"distance_{group}"] = values[candidates["_source_pos"].to_numpy(dtype=int)]

    feature_trace = pd.DataFrame(feature_rows)
    if len(feature_trace):
        feature_trace = feature_trace.merge(
            candidates[["date", "rank", "distance"]],
            on="date",
            how="inner",
        )
    return candidates.drop(columns=["_source_pos"]), feature_trace


def _explain_hour_candidates(
    query: pd.Series,
    work: pd.DataFrame,
    spec: ModelSpec,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidates: list[pd.DataFrame] = []
    feature_rows: list[dict] = []
    flt_radius = int(spec.flt_radius)

    for hour in HOURS:
        cols_present = [
            c for c in _window_columns(hour, flt_radius)
            if c in work.columns and c in query.index
        ]
        if not cols_present:
            continue

        pool_vals = work[cols_present].to_numpy(dtype=float)
        query_vals = query[cols_present].to_numpy(dtype=float)
        means, stds = _zscore_fit(pool_vals)
        pool_z = (pool_vals - means) / stds
        query_z = ((query_vals - means) / stds).reshape(-1)

        diff = query_z - pool_z
        mask = ~np.isnan(diff)
        sq = np.where(mask, diff ** 2, 0.0)
        n_valid = mask.sum(axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            distance = np.where(n_valid > 0, np.sqrt(sq.sum(axis=1) / n_valid), np.inf)

        hour_candidates = work[["date"]].copy()
        hour_candidates["_source_pos"] = np.arange(len(work))
        hour_candidates["hour_ending"] = hour
        hour_candidates["distance"] = distance
        hour_candidates["active_weight_sum"] = 1.0
        hour_candidates["distance_load_window"] = distance
        hour_candidates = hour_candidates[np.isfinite(hour_candidates["distance"])].copy()
        hour_candidates = hour_candidates.sort_values(
            ["distance", "date"], ascending=[True, False],
        ).reset_index(drop=True)
        hour_candidates["rank"] = np.arange(1, len(hour_candidates) + 1)
        candidates.append(hour_candidates)

        for i in range(len(work)):
            valid_idx = np.where(mask[i])[0]
            if len(valid_idx) == 0 or not np.isfinite(distance[i]):
                continue
            for j in valid_idx:
                feature_rows.append({
                    "date": work.iloc[i]["date"],
                    "hour_ending": hour,
                    "group": "load_window",
                    "group_weight": 1.0,
                    "feature": cols_present[j],
                    "target_value": query_vals[j],
                    "candidate_value": pool_vals[i, j],
                    "pool_mean": means[j],
                    "pool_std": stds[j],
                    "target_z": query_z[j],
                    "candidate_z": pool_z[i, j],
                    "z_delta": diff[i, j],
                    "abs_z_delta": abs(diff[i, j]),
                    "squared_delta": diff[i, j] ** 2,
                    "n_valid_in_group": int(n_valid[i]),
                    "group_distance": float(distance[i]),
                    "weighted_group_distance": float(distance[i]),
                })

    cand = pd.concat(candidates, ignore_index=True) if candidates else pd.DataFrame()
    trace = pd.DataFrame(feature_rows)
    if len(cand) and len(trace):
        trace = trace.merge(
            cand[["date", "hour_ending", "rank", "distance"]],
            on=["date", "hour_ending"],
            how="inner",
        )
    return cand.drop(columns=["_source_pos"], errors="ignore"), trace


def _rank_candidates(
    work: pd.DataFrame,
    weighted_sum: np.ndarray,
    weight_sum: np.ndarray,
) -> pd.DataFrame:
    distances = np.full(len(work), np.inf, dtype=float)
    valid = weight_sum > 0
    distances[valid] = weighted_sum[valid] / weight_sum[valid]

    candidates = work.copy()
    candidates["_source_pos"] = np.arange(len(work))
    candidates["distance"] = distances
    candidates["active_weight_sum"] = weight_sum
    candidates = candidates[np.isfinite(candidates["distance"])].copy()
    candidates = candidates.sort_values(["distance", "date"], ascending=[True, False])
    candidates = candidates.reset_index(drop=True)
    candidates["rank"] = np.arange(1, len(candidates) + 1)
    return candidates


def _build_day_picks(
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    analogs: pd.DataFrame,
    candidates: pd.DataFrame,
) -> pd.DataFrame:
    distance_cols = [c for c in candidates.columns if c.startswith("distance_")]
    detail = candidates[["date", "active_weight_sum"] + distance_cols].drop_duplicates("date")
    picks = analogs.merge(detail, on="date", how="left")
    picks = picks.rename(columns={"date": "analog_date"})
    picks = _add_run_columns(picks, run_id, target_date, config, spec)
    picks["match_unit"] = "day"
    picks["hour_ending"] = np.nan
    picks["analog_date"] = picks["analog_date"].astype(str)
    picks["weight_pct"] = pd.to_numeric(picks["weight"], errors="coerce") / picks["weight"].sum()
    _add_top_distance_group(picks)
    return picks


def _build_hour_picks(
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    analogs: pd.DataFrame,
    candidates: pd.DataFrame,
) -> pd.DataFrame:
    detail = candidates[
        ["date", "hour_ending", "active_weight_sum", "distance_load_window"]
    ].drop_duplicates(["date", "hour_ending"])
    picks = analogs.merge(detail, on=["date", "hour_ending"], how="left")
    picks = picks.rename(columns={"date": "analog_date"})
    picks = _add_run_columns(picks, run_id, target_date, config, spec)
    picks["match_unit"] = "hour"
    picks["analog_date"] = picks["analog_date"].astype(str)
    picks["weight_pct"] = picks.groupby("hour_ending")["weight"].transform(
        lambda s: pd.to_numeric(s, errors="coerce") / s.sum(),
    )
    picks["top_distance_group"] = "load_window"
    return picks


def _add_run_columns(
    df: pd.DataFrame,
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
) -> pd.DataFrame:
    out = df.copy()
    out.insert(0, "run_id", run_id)
    out.insert(1, "target_date", str(target_date))
    out.insert(2, "model_name", spec.name)
    out.insert(3, "hub", config.hub)
    return out


def _add_top_distance_group(picks: pd.DataFrame) -> None:
    distance_cols = [c for c in picks.columns if c.startswith("distance_")]
    if distance_cols:
        picks["top_distance_group"] = (
            picks[distance_cols]
            .astype(float)
            .idxmax(axis=1)
            .str.replace("distance_", "", regex=False)
        )


def _build_selected_day_trace(
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    feature_trace: pd.DataFrame,
    selected_dates: set[str],
) -> pd.DataFrame:
    if len(feature_trace) == 0:
        return pd.DataFrame()
    trace = feature_trace.copy()
    trace["analog_date"] = pd.to_datetime(trace["date"]).dt.date.astype(str)
    trace = trace[trace["analog_date"].isin(selected_dates)].copy()
    return _finish_trace(trace, run_id, target_date, config, spec)


def _build_selected_hour_trace(
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    feature_trace: pd.DataFrame,
    selected: set[tuple[int, str]],
) -> pd.DataFrame:
    if len(feature_trace) == 0:
        return pd.DataFrame()
    trace = feature_trace.copy()
    trace["analog_date"] = pd.to_datetime(trace["date"]).dt.date.astype(str)
    keep = [
        (int(hour), str(analog_date)) in selected
        for hour, analog_date in zip(trace["hour_ending"], trace["analog_date"])
    ]
    trace = trace.loc[keep].copy()
    return _finish_trace(trace, run_id, target_date, config, spec)


def _finish_trace(
    trace: pd.DataFrame,
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
) -> pd.DataFrame:
    trace = trace.drop(columns=["date"], errors="ignore")
    trace.insert(0, "run_id", run_id)
    trace.insert(1, "target_date", str(target_date))
    trace.insert(2, "model_name", spec.name)
    trace.insert(3, "hub", config.hub)
    trace["feature_distance_contribution"] = (
        pd.to_numeric(trace["group_weight"], errors="coerce")
        * pd.to_numeric(trace["squared_delta"], errors="coerce")
        / pd.to_numeric(trace["n_valid_in_group"], errors="coerce")
    )
    return trace


def _build_day_contributions(
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    picks: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict] = []
    for _, row in picks.iterrows():
        weight = float(row["weight"])
        for hour in HOURS:
            lmp = row.get(f"lmp_h{hour}")
            if pd.isna(lmp):
                continue
            rows.append(_contribution_row(
                run_id, target_date, config, spec, row, hour, weight, float(lmp),
            ))
    return pd.DataFrame(rows)


def _build_hour_contributions(
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    picks: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict] = []
    for _, row in picks.iterrows():
        lmp = row.get("lmp")
        if pd.isna(lmp):
            continue
        hour = int(row["hour_ending"])
        weight = float(row["weight"])
        rows.append(_contribution_row(
            run_id, target_date, config, spec, row, hour, weight, float(lmp),
        ))
    return pd.DataFrame(rows)


def _contribution_row(
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    row: pd.Series,
    hour: int,
    weight: float,
    lmp: float,
) -> dict:
    return {
        "run_id": run_id,
        "target_date": str(target_date),
        "model_name": spec.name,
        "hub": config.hub,
        "match_unit": spec.match_unit,
        "analog_date": str(row["analog_date"]),
        "rank": int(row["rank"]),
        "hour_ending": hour,
        "weight": weight,
        "weight_pct": float(row["weight_pct"]),
        "analog_lmp": lmp,
        "lmp_contribution": weight * lmp,
    }


def _build_feature_price_correlations(
    run_id: str,
    target_date: date,
    config: KnnModelConfig,
    spec: ModelSpec,
    pool: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict] = []
    for group, features in _correlation_feature_groups(spec).items():
        for feature in features:
            if feature not in pool.columns:
                continue
            x = pd.to_numeric(pool[feature], errors="coerce")
            for hour in HOURS:
                lmp_col = f"lmp_h{hour}"
                if lmp_col not in pool.columns:
                    continue
                y = pd.to_numeric(pool[lmp_col], errors="coerce")
                valid = x.notna() & y.notna()
                rows.append({
                    "run_id": run_id,
                    "target_date": str(target_date),
                    "model_name": spec.name,
                    "hub": config.hub,
                    "group": group,
                    "feature": feature,
                    "hour_ending": hour,
                    "n": int(valid.sum()),
                    "pearson_corr": _corr(x[valid], y[valid], "pearson"),
                    "spearman_corr": _corr(x[valid], y[valid], "spearman"),
                })
    return pd.DataFrame(rows)


def _correlation_feature_groups(spec: ModelSpec) -> dict[str, list[str]]:
    if spec.match_unit == "hour":
        return {"load_hour": [f"fcst_load_h{h}" for h in HOURS]}
    return spec.feature_groups


def _window_columns(target_hour: int, flt_radius: int) -> list[str]:
    lo = max(1, target_hour - flt_radius)
    hi = min(24, target_hour + flt_radius)
    return [f"fcst_load_h{h}" for h in range(lo, hi + 1)]


def _zscore_fit(arr: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    means = np.nanmean(arr, axis=0)
    stds = np.nanstd(arr, axis=0)
    stds = np.where(stds == 0, 1.0, stds)
    return means, stds


def _corr(x: pd.Series, y: pd.Series, method: str) -> float:
    if len(x) < 3 or x.nunique(dropna=True) < 2 or y.nunique(dropna=True) < 2:
        return np.nan
    return float(x.corr(y, method=method))


def _write_table(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
