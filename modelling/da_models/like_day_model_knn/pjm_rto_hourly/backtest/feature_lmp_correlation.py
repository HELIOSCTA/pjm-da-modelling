"""Single-day feature-LMP correlation diagnostic for pjm_rto_hourly.

For one target date, prints the 24-hour profile of each within-day
feature alongside the actual DA LMP in the canonical
``Date | Type | HE1..HE24 | OnPeak | OffPeak | Flat`` layout, then
prints the within-day Pearson correlation between each feature and LMP.

Features evaluated:

    load        - load_h1..load_h24 (gross RTO load)
    solar       - solar_h1..solar_h24
    wind        - wind_h1..wind_h24
    net_load    - load_h - solar_h.fillna(0) - wind_h.fillna(0)

``net_load`` is computed inline; no domain registration needed. Treating
missing renewables as zero is defensible pre-2019 (small share of stack)
and a non-issue for recent dates.

Correlation interpretation: this is a **within-day shape correlation** —
``corr(feature_h{1..24}, lmp_h{1..24})`` for the single target date.
Both Pearson and Spearman (rank) are reported. Pearson measures linear
shape match; Spearman is robust to single-hour LMP spikes that can
dominate Pearson on n=24. Neither is a per-hour cross-day correlation.

Broadcast features (outages, gas) have no within-day shape and are
excluded from this diagnostic.

Reads only ``pool`` rows (no engine call), so it returns in seconds.

Usage::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.backtest.feature_lmp_correlation
    python modelling/da_models/like_day_model_knn/pjm_rto_hourly/backtest/feature_lmp_correlation.py
"""

from __future__ import annotations

import sys
import time
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.common.data import loader  # noqa: E402
from da_models.common.forecast.output import (  # noqa: E402
    actuals_from_pool,
    add_summary_cols,
)
from da_models.common.stats.correlation import pearson as _pearson  # noqa: E402
from da_models.common.stats.correlation import spearman as _spearman  # noqa: E402
from da_models.like_day_model_knn import configs  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.builder import build_pool  # noqa: E402
from utils.logging_utils import (  # noqa: E402
    Colors,
    supports_color,
)

_COLOR_ON: bool = supports_color()
_HL_LMP: str = (Colors.BOLD + Colors.BRIGHT_RED) if _COLOR_ON else ""
_HL_SEP: str = Colors.BRIGHT_CYAN if _COLOR_ON else ""
_HL_LEADER: str = Colors.BOLD if _COLOR_ON else ""
_HL_WIN: str = Colors.BRIGHT_GREEN if _COLOR_ON else ""
_HL_LOSS: str = Colors.BRIGHT_RED if _COLOR_ON else ""
_HL_DIM: str = Colors.DIM if _COLOR_ON else ""
_RS: str = Colors.RESET if _COLOR_ON else ""


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
# None -> most recent date in pool with full LMP actuals (typically yesterday).
TARGET_DATE: date | None = date(2026, 5, 6)
# Defaults to the FULL spec so the pool includes net_load_h{1..24} columns
# from rto_net_load_profile (which reads from the unified supply-demand
# coalescer). All four supply-demand features therefore share a single
# (forecast | RT) decision per (region, date) — net_load is read directly
# from the pool, not derived from load/solar/wind, eliminating the
# cross-source mixing artifact.
MODEL_NAME: str = configs.PJM_RTO_HOURLY_FULL_SPEC.name
HUB: str = configs.HUB

# Within-day features to score. All map to pool columns
# ``{stem}_h1..{stem}_h24`` after the FULL-spec migration; net_load is
# sourced from the unified loader rather than derived.
FEATURES: tuple[str, ...] = ("load", "solar", "wind", "net_load")

_HOURS: list[int] = list(range(1, 25))
_LMP_COLS: list[str] = [f"lmp_h{h}" for h in _HOURS]
_SEP_VALUE_COL: str = "lmp_system_energy_price"


def _load_sep_profile(
    target_date: date,
    cache_dir: Path | None,
    hub: str,
) -> np.ndarray | None:
    """Pull the 24-hour SEP profile for ``target_date`` at ``hub`` directly
    from the parquet via ``loader.load_lmp_system_energy_da``.

    Sidesteps the pool/labels path so this diagnostic stays self-contained.
    SEP is system-wide, so any hub yields the same series; we filter to one
    hub purely to dedupe to a single row per (date, HE).

    Returns ``None`` if the parquet is missing or coverage for the target
    date is partial — caller falls back to LMP-only correlation.
    """
    try:
        df = loader.load_lmp_system_energy_da(cache_dir=cache_dir)
    except (FileNotFoundError, RuntimeError):
        return None
    df = df[(df["region"].astype(str) == hub) & (df["date"] == target_date)]
    if df.empty:
        return None
    sub = df.set_index("hour_ending")[_SEP_VALUE_COL]
    arr = np.array([float(sub.get(h, np.nan)) for h in _HOURS], dtype=float)
    if not np.isfinite(arr).all():
        return None
    return arr


# Bucket -> hours, mirroring common.forecast.output.add_summary_cols.
# OnPk = HE8..HE23 (16 hrs); OffPk = HE1..HE7 + HE24 (8 hrs); Flat = all 24.
_HOUR_BUCKETS: dict[str, list[int]] = {
    "OnPk": list(range(8, 24)),
    "OffPk": list(range(1, 8)) + [24],
    "Flat": list(_HOURS),
}


def _resolve_target_date(pool: pd.DataFrame, target_date: date | None) -> date:
    """Return the target date, defaulting to the most recent pool date with
    full LMP actuals when ``target_date`` is None."""
    if target_date is not None:
        return target_date
    candidates = sorted(pool["date"].unique(), reverse=True)
    for d in candidates:
        d = d if isinstance(d, date) else pd.Timestamp(d).date()
        if actuals_from_pool(pool, d) is not None:
            return d
    raise RuntimeError(
        "No pool dates with full LMP actuals; cannot default target_date."
    )


def _feature_profile(row: pd.Series, feature: str) -> np.ndarray | None:
    """Pull the 24-hour profile for ``feature`` off a pool row.

    Returns ``None`` if the underlying columns are missing entirely.
    All four supply-demand features (load/solar/wind/net_load) come from
    the FULL spec's pool, which sources every series from the unified
    ``load_pjm_supply_demand_coalesced`` — so net_load is read directly,
    not derived, and the identity ``net_load = load - solar - wind``
    holds within each row by construction.
    """
    cols = [f"{feature}_h{h}" for h in _HOURS]
    if not all(c in row.index for c in cols):
        return None
    return np.array([row[c] for c in cols], dtype=float)


def _lmp_profile(row: pd.Series) -> np.ndarray | None:
    if not all(c in row.index for c in _LMP_COLS):
        return None
    return np.array([row[c] for c in _LMP_COLS], dtype=float)


def _profile_table(
    row: pd.Series,
    lmp: np.ndarray,
    sep: np.ndarray | None,
    features: tuple[str, ...],
    target_date: date,
) -> pd.DataFrame:
    """Wide table: one row per feature, then ``lmp``, then ``sep`` (if
    available). Columns: ``Date | Type | HE1..HE24 | OnPeak | OffPeak | Flat``."""
    targets: list[tuple[str, np.ndarray]] = [("lmp", lmp)]
    if sep is not None:
        targets.append(("sep", sep))
    target_map = dict(targets)

    rows: list[dict] = []
    for f in (*features, *[name for name, _ in targets]):
        if f in target_map:
            prof = target_map[f]
        else:
            prof = _feature_profile(row, f)
            if prof is None:
                prof = np.full(24, np.nan, dtype=float)
        rec: dict = {"Date": str(target_date), "Type": f}
        for h in _HOURS:
            v = prof[h - 1]
            rec[f"HE{h}"] = float(v) if pd.notna(v) else float("nan")
        rec = add_summary_cols(rec)
        rows.append(rec)
    return pd.DataFrame(rows)


def _correlations(
    profile: pd.DataFrame,
    features: tuple[str, ...],
    has_sep: bool,
    corr_fn=_pearson,
) -> pd.DataFrame:
    """Per-feature within-day correlation against ``lmp_total`` (always)
    and ``sep`` (when ``has_sep``), broken out by OnPk / OffPk / Flat hour
    buckets. Sorted by |corr_lmp_flat| desc.

    ``corr_fn`` selects the correlation method: ``_pearson`` (default,
    linear) or ``_spearman`` (rank; robust to single-hour LMP spikes).
    """
    lmp_row = profile[profile["Type"] == "lmp"].iloc[0]
    sep_row = profile[profile["Type"] == "sep"].iloc[0] if has_sep else None

    rows: list[dict] = []
    for f in features:
        feat_row = profile[profile["Type"] == f].iloc[0]
        rec: dict = {"feature": f}
        for bucket, hours in _HOUR_BUCKETS.items():
            b = bucket.lower()
            prof = np.array([feat_row[f"HE{h}"] for h in hours], dtype=float)
            lmp_v = np.array([lmp_row[f"HE{h}"] for h in hours], dtype=float)
            rec[f"corr_lmp_{b}"] = corr_fn(prof, lmp_v)
            if sep_row is not None:
                sep_v = np.array([sep_row[f"HE{h}"] for h in hours], dtype=float)
                rec[f"corr_sep_{b}"] = corr_fn(prof, sep_v)
            rec[f"n_{b}"] = int((np.isfinite(prof) & np.isfinite(lmp_v)).sum())
        rows.append(rec)
    df = pd.DataFrame(rows)
    df["_abs_lmp_flat"] = df["corr_lmp_flat"].abs()
    df = (
        df.sort_values("_abs_lmp_flat", ascending=False, na_position="last")
        .drop(columns="_abs_lmp_flat")
        .reset_index(drop=True)
    )
    return df


def _format_cell(val: float | None, width: int, is_price: bool) -> str:
    """Format a single cell. ``is_price`` -> signed 2-decimal $/MWh
    (lmp/sep); otherwise comma-grouped integer (load/solar/wind MW)."""
    if val is None or pd.isna(val):
        return f" {'':>{width}}"
    if is_price:
        return f" {val:>+{width}.2f}"
    return f" {val:>{width},.0f}"


_DOW_ABBR: tuple[str, ...] = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def _print_config(
    target_date: date,
    hub: str,
    model_name: str,
    n_pool: int,
    features: tuple[str, ...],
    has_sep: bool,
    width: int = 120,
) -> None:
    """Configuration block — surfaces what was actually measured at a glance.

    Lists the spec / pool source, target date, hub, features under test,
    and targets so anyone reading the output can tell exactly which
    features came from which pool, what they were correlated against, and
    on what date — without having to read the script.
    """
    from utils.logging_utils import print_divider, print_header

    spec = configs.MODEL_REGISTRY.get(model_name)
    domain_list = ", ".join(spec.domains) if spec is not None else "?"
    target_dow = _DOW_ABBR[target_date.weekday()]
    targets_str = "lmp_total, sep" if has_sep else "lmp_total"

    print()
    print_header("FEATURE -> LMP CORRELATION CONFIGURATION", "=", width)
    print()
    print(f"  Target date     {target_date}  ({target_dow})")
    print(f"  Hub             {hub}  (used to filter SEP rows)")
    print(f"  Spec            {model_name}")
    print(f"  Domains         {domain_list}")
    print(
        "  Pool source     load_pjm_supply_demand_coalesced  "
        "(RTO; single source decision per date)"
    )
    print(f"  Pool size       {n_pool:,} rows")
    print(f"  Features        {', '.join(features)}")
    print(
        "                  (within-day shape features only "
        "-- outages/gas excluded as broadcast)"
    )
    print(f"  Targets         {targets_str}")
    print()
    print_divider("=", width, dim=False)


def _print_scope(
    features: tuple[str, ...],
    has_sep: bool,
    model_name: str,
) -> None:
    """Scope block — explicitly names what's under correlation analysis.

    Distinct from the configuration block (which lists what's in the
    spec). This block answers: 'of all the spec's groups, which N
    features are THIS diagnostic actually correlating against LMP/SEP,
    and which are excluded?' Surfaces the windowed/broadcast distinction.

    Spec feature-group weights are NOT used here — this diagnostic
    computes raw (unweighted) Pearson/Spearman against LMP/SEP. Weights
    drive the KNN engine's analog selection, not within-day correlation.
    """
    from utils.logging_utils import print_section

    spec = configs.MODEL_REGISTRY.get(model_name)
    feature_prefixes = tuple(f"{f}_" for f in features)
    excluded: list[str] = []
    if spec is not None:
        for g in spec.feature_groups:
            if not any(g.startswith(p) for p in feature_prefixes):
                excluded.append(g)
    excluded_str = ", ".join(sorted(set(excluded))) if excluded else "(none)"

    n_features = len(features)
    n_targets = 2 if has_sep else 1
    targets = "lmp_total, sep" if has_sep else "lmp_total"

    print_section(f"Correlation scope: {n_features} features x {n_targets} target(s)")
    print(f"  Features under test    {', '.join(features)}")
    print(f"  Targets                {targets}")
    print(
        "  Buckets                OnPk (HE8..HE23, 16h)"
        " | OffPk (HE1..HE7 + HE24, 8h) | Flat (24h)"
    )
    print(f"  Excluded broadcasts    {excluded_str}")
    print("                         (single daily value, no within-day shape;")
    print(
        "                         within-day Pearson is undefined against a constant)"
    )


def _print_profile(profile: pd.DataFrame, target_date: date, hub: str) -> None:
    """24-hour profile table. Highlights the ``lmp`` row (bold red) and the
    ``sep`` row (cyan) so the labels visually pop above the feature rows."""
    from utils.logging_utils import print_divider, print_header

    he_w = 7
    summary_w = 9

    header = f"{'Date':<12} {'Type':<10}"
    for h in _HOURS:
        header += f" {h:>{he_w}}"
    header += f" {'OnPk':>{summary_w}} {'OffPk':>{summary_w}} {'Flat':>{summary_w}}"
    table_w = len(header)
    banner_w = 120  # banner narrower than the data table; matches printers.py

    print_header(f"24-HOUR PROFILE  --  {target_date}  ({hub})", "=", banner_w)
    print()
    print(header)
    print_divider("-", table_w, dim=False)

    for _, row in profile.iterrows():
        is_lmp = row["Type"] == "lmp"
        is_sep = row["Type"] == "sep"
        is_price = is_lmp or is_sep
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in _HOURS:
            line += _format_cell(row.get(f"HE{h}"), he_w, is_price)
        for col in ("OnPeak", "OffPeak", "Flat"):
            line += _format_cell(row.get(col), summary_w, is_price)
        if is_lmp:
            line = f"{_HL_LMP}{line}{_RS}"
        elif is_sep:
            line = f"{_HL_SEP}{line}{_RS}"
        print(line)

    print_divider("-", table_w, dim=False)


def _print_correlations(
    profile: pd.DataFrame,
    corr_df: pd.DataFrame,
    target_date: date,
    has_sep: bool,
    method: str = "Pearson",
) -> None:
    """Correlation table only — no comparison/leader callouts.

    Uses the printers.py-style banner / section helpers; bolds the
    leader row in the corr table. ``method`` is the label printed in
    the banner ("Pearson" / "Spearman").
    """
    from utils.logging_utils import print_divider, print_header

    width = 130 if has_sep else 100

    print()
    print_header(
        f"FEATURE -> LMP CORRELATION ({method.upper()})  --  {target_date}",
        "=",
        width,
    )
    print(
        f"  {method} within-day shape match across the HEs in each bucket "
        "(this date only; not cross-day)."
    )
    if method == "Spearman":
        print(
            "  Rank correlation; robust to single-hour LMP spikes that can "
            "dominate Pearson on n=24."
        )
    if has_sep:
        print(
            "  Compares features against lmp_total AND lmp_system_energy_price (SEP)."
        )
        print("  SEP isolates fundamentals from congestion + losses; if a feature's")
        print(
            "  correlation rises lmp -> sep, congestion/loss noise was diluting the match."
        )
    print(
        "  OnPk = HE8..HE23 (16 hrs)   OffPk = HE1..HE7 + HE24 (8 hrs)   Flat = all 24 hrs"
    )
    print_divider("=", width, dim=False)

    leader_feature: str | None = None
    if len(corr_df) >= 1 and pd.notna(corr_df.iloc[0]["corr_lmp_flat"]):
        leader_feature = str(corr_df.iloc[0]["feature"])

    corr_w = 8

    def _corr_cell(v: float | None) -> str:
        if pd.isna(v):
            return f"{'n/a':>{corr_w}}"
        return f"{v:>+{corr_w}.3f}"

    def _n_cell(v) -> str:
        return f"{int(v):>6d}"

    feat_w = max(8, max((len(str(f)) for f in corr_df["feature"]), default=8))

    def _row_str(r: pd.Series) -> str:
        parts = [f"  {str(r['feature']):<{feat_w}}"]
        # vs lmp_total block
        parts.append(_corr_cell(r["corr_lmp_onpk"]))
        parts.append(_corr_cell(r["corr_lmp_offpk"]))
        parts.append(_corr_cell(r["corr_lmp_flat"]))
        if has_sep:
            parts.append(" |")
            parts.append(_corr_cell(r.get("corr_sep_onpk")))
            parts.append(_corr_cell(r.get("corr_sep_offpk")))
            parts.append(_corr_cell(r.get("corr_sep_flat")))
        parts.append(" |")
        parts.append(_n_cell(r["n_flat"]))
        return "  ".join(parts)

    # Header rows: top is "vs lmp_total | vs SEP" group label, second is bucket names.
    block_w = corr_w * 3 + 4  # 3 corr cells + 2 inter-cell spaces
    parts_top = [f"  {'':<{feat_w}}", f"{'-- vs lmp_total --':^{block_w}}"]
    if has_sep:
        parts_top.append(f"{'':<2}")
        parts_top.append(f"{'-- vs SEP --':^{block_w}}")
    parts_top.append(f"{'':<2}")
    parts_top.append(f"{'n':>6}")
    print()
    print("  ".join(parts_top))

    parts_hdr = [f"  {'feature':<{feat_w}}"]
    parts_hdr.extend(
        [f"{'onpk':>{corr_w}}", f"{'offpk':>{corr_w}}", f"{'flat':>{corr_w}}"]
    )
    if has_sep:
        parts_hdr.append(" |")
        parts_hdr.extend(
            [f"{'onpk':>{corr_w}}", f"{'offpk':>{corr_w}}", f"{'flat':>{corr_w}}"]
        )
    parts_hdr.append(" |")
    parts_hdr.append(f"{'HEs':>6}")
    print("  ".join(parts_hdr))
    print_divider("-", width, dim=False)

    for _, r in corr_df.iterrows():
        line = _row_str(r)
        if leader_feature is not None and r["feature"] == leader_feature:
            line = f"{_HL_LEADER}{line}{_RS}"
        print(line)
    print_divider("-", width, dim=False)

    print()


def run(
    target_date: date | None = TARGET_DATE,
    features: tuple[str, ...] = FEATURES,
    model_name: str = MODEL_NAME,
) -> dict:
    """Single-day within-day feature-LMP correlation diagnostic.

    Returns ``{"profile": DataFrame, "correlations": DataFrame,
    "target_date": date}``. Prints the 24-hour profile and correlation
    table to stdout. No artefacts written.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    base_spec = configs.MODEL_REGISTRY[model_name]
    spec_for_build = replace(base_spec, flt_radius=int(base_spec.flt_radius))

    print("\n[corr] building pool...")
    t0 = time.perf_counter()
    pool = build_pool(spec=spec_for_build, cache_dir=configs.CACHE_DIR)
    print(f"[corr] pool built in {time.perf_counter() - t0:.1f}s ({len(pool)} rows)")

    resolved = _resolve_target_date(pool, target_date)
    if target_date is None:
        print(f"[corr] target_date=None -> most recent w/ actuals: {resolved}")
    else:
        print(f"[corr] target_date={resolved}")
    print(f"[corr] features: {features}")

    pool_indexed = pool.set_index("date", drop=False)
    if resolved not in pool_indexed.index:
        raise RuntimeError(f"target_date={resolved} not present in pool.")
    row = pool_indexed.loc[resolved]
    if isinstance(row, pd.DataFrame):
        row = row.iloc[0]

    lmp = _lmp_profile(row)
    if lmp is None or not np.isfinite(lmp).all():
        raise RuntimeError(
            f"target_date={resolved} is missing one or more lmp_h* values; "
            f"pick a date with full DA LMP actuals."
        )

    sep = _load_sep_profile(resolved, configs.CACHE_DIR, HUB)
    if sep is None:
        print(
            "[corr] SEP unavailable for this date/hub -- correlations vs SEP skipped."
        )
    else:
        print(f"[corr] SEP loaded for {HUB}: 24/24 HEs")

    profile = _profile_table(row, lmp, sep, features, resolved)
    corr_df = _correlations(profile, features, has_sep=sep is not None)

    _print_config(
        target_date=resolved,
        hub=HUB,
        model_name=model_name,
        n_pool=len(pool),
        features=features,
        has_sep=sep is not None,
    )
    _print_scope(features=features, has_sep=sep is not None, model_name=model_name)
    _print_profile(profile, resolved, HUB)
    _print_correlations(profile, corr_df, resolved, has_sep=sep is not None)

    spearman_df = _correlations(
        profile, features, has_sep=sep is not None, corr_fn=_spearman
    )
    _print_correlations(
        profile,
        spearman_df,
        resolved,
        has_sep=sep is not None,
        method="Spearman",
    )

    return {
        "profile": profile,
        "correlations": corr_df,
        "correlations_spearman": spearman_df,
        "target_date": resolved,
    }


# Silence ruff F401 for the timedelta import retained for future
# extensions (e.g. supporting TARGET_DATE = date.today() - timedelta(days=N)).
_ = timedelta


if __name__ == "__main__":
    run()
