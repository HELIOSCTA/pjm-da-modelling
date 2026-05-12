"""ICE-anchored single-day Meteologica DA-price baseline.

Sibling of ``forecast_single_day.py``. Same loader, same fan, same
printers — but the summary table additionally carries four anchored
rows (``Det (ICE)``, ``ENS Avg (ICE)``, ``ENS Bottom (ICE)``,
``ENS Top (ICE)``) computed by multiplicatively scaling each
Meteologica series so its OnPeak (HE8-23) mean matches the ICE PDA
D1-IUS VWAP for the same delivery date.

Anchor formula (per series):
    scale_s = ICE_VWAP / mean_s(HE8..HE23)
    anchored_HE_h_s = meteo_HE_h_s * scale_s   (h ∈ 1..24)

Preserves Meteologica's hourly shape ratios; rebases the level to
where ICE is currently clearing. The 51-member ENS fan in the second
table is **never** anchored — it remains Meteologica's published
distribution.

When no ICE trades exist for the target date (or all are excluded
because direction ∉ {Lift, Hit}), the script falls back to the
unanchored Phase 1 layout and warns.

Tunable defaults live in module-level constants — edit directly or
override via ``run(...)`` from a notebook.

Usage::

    python -m backend.modelling.da_models.baseline_meteo_da_price.pipelines.forecast_single_day_ice_anchored
"""

from __future__ import annotations

import sys
import uuid
from datetime import date, timedelta
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[5]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from backend.modelling.da_models.baseline_meteo_da_price import ice_anchor  # noqa: E402
from backend.modelling.da_models.baseline_meteo_da_price import printers as _phase1_printers  # noqa: E402
from backend.modelling.da_models.baseline_meteo_da_price.printers import (  # noqa: E402
    SERIES_TO_COL,
    build_bands_table,
    build_bands_vs_actuals,
    build_forecast_vs_actuals,
    build_members_table,
    compute_dispersion_metrics,
    print_bands_section,
    print_bands_vs_actuals_section,
    print_forecast_vs_actuals_section,
)
from backend.modelling.da_models.common.publish import publish_forecast_run  # noqa: E402
from backend.modelling.da_models.common.data import loader  # noqa: E402
from backend.modelling.da_models.common.forecast.output import actuals_from_pool  # noqa: E402
from backend.utils.logging_utils import (  # noqa: E402
    Colors,
    init_logging,
    print_divider,
    print_header,
    print_section,
    supports_color,
)

# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = None  # None -> tomorrow
# Forecast vintage -- the date the run is produced (None -> date.today()).
RUN_DATE: date | None = None
HUB: str = "WESTERN HUB"
LEAD_DAYS: int | None = 1  # DA-cutoff vintage; None for all vintages
CACHE_DIR: Path | None = None
ICE_SYMBOL: str = ice_anchor.DEFAULT_SYMBOL  # "PDA D1-IUS"
ICE_VWAP_CUTOFF: pd.Timestamp | None = None  # None -> all trades to date
LOG_DIR: Path = _REPO_ROOT / "backend" / "modelling" / "logs"
# Frontend ingestion key — stable identifier for this specific model.
MODEL_NAME: str = "baseline_meteo_da_price_ice_anchored"
# Family bucket used by the frontend tabs to group runs across model
# variants (e.g. an unanchored Meteo baseline would also be "baseline").
MODEL_FAMILY: str = "baseline"
# The pipeline always publishes the run to pjm_model_outputs.forecast_runs
# (one row, upserted via backend.modelling.da_models.common.publish.publish_forecast_run) so the
# frontend can read it. Batch/backtest callers that must NOT write a row per
# date pass publish=False to run().
PUBLISH: bool = True

_COLOR_ON = supports_color()
_RS = Colors.RESET if _COLOR_ON else ""

# Anchored rows wear the same Type labels as their raw counterparts —
# the section title ("Scaled to ICE" vs "Unscaled (raw Meteo)") carries
# the distinction, and identical labels keep both tables the same column
# width. Phase 1's _ROW_STYLES already has entries for these labels, so
# anchored rows inherit the same per-series colors.
_ANCHORED_LABEL_FOR_SERIES: dict[str, str] = {
    label: label for label in ("Det", "ENS Avg", "ENS Bottom", "ENS Top")
}


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def _first_or_none(s: pd.Series) -> pd.Timestamp | None:
    s = s.dropna()
    return None if s.empty else pd.Timestamp(s.iloc[0])


def _series_hourly_dicts(df: pd.DataFrame) -> dict[str, dict[int, float]]:
    """Map each named series label -> {hour_ending: value}."""
    return {
        label: _phase1_printers._hourly_dict_from_df(df, col)
        for label, col in SERIES_TO_COL.items()
    }


# Order in which we try to anchor: Det is the primary point forecast;
# ENS Avg is the fallback if Det's OnPeak is unusable (very rare).
_SHARED_ANCHOR_PRIORITY: tuple[str, ...] = ("Det", "ENS Avg")


def _compute_shared_multiplier(
    series_hourly: dict[str, dict[int, float]],
    ice_vwap: float,
) -> tuple[float | None, str | None]:
    """Single shared anchor — derived from Det (or ENS Avg fallback).

    Returns ``(scale, anchor_label)``. ``(None, None)`` if neither
    Det nor ENS Avg has a usable OnPeak mean. The same scale is then
    applied to all four series so Meteo's full uncertainty width is
    preserved on every HE while the level rebases to ICE.
    """
    for label in _SHARED_ANCHOR_PRIORITY:
        hourly = series_hourly.get(label)
        if not hourly:
            continue
        try:
            return ice_anchor.onpeak_multiplier(hourly, ice_vwap), label
        except ValueError:
            continue
    return None, None


def _implied_per_series_multipliers(
    series_hourly: dict[str, dict[int, float]],
    ice_vwap: float,
) -> dict[str, float]:
    """Diagnostic only — the alternative (per-series-anchored) scales.

    Shown dim in ``print_anchored_config`` so the trader can see how
    each Meteo series would have moved if anchored independently. Not
    applied to the forecast (would collapse the uncertainty band).
    """
    out: dict[str, float] = {}
    for label, hourly in series_hourly.items():
        try:
            out[label] = ice_anchor.onpeak_multiplier(hourly, ice_vwap)
        except ValueError:
            continue
    return out


def _scale_forecast_df(df: pd.DataFrame, scale: float) -> pd.DataFrame:
    """Return a copy of ``df`` with every Meteologica price column
    multiplied by ``scale``. Non-price columns (date, hour_ending,
    as_of_date, exec timestamps) pass through untouched.

    Used to anchor the members fan to ICE: multiplying every series
    (Det, ENS Avg, Bottom, Top, and the 51 individual ECMWF members)
    by the same shared scale rebases the level while preserving the
    full ensemble dispersion.
    """
    out = df.copy()
    price_cols = [c for c in out.columns if c.startswith("da_price_")]
    for c in price_cols:
        out[c] = out[c].astype(float) * float(scale)
    return out


def _anchored_series_hourly(
    series_hourly: dict[str, dict[int, float]],
    multipliers: dict[str, float],
) -> dict[str, dict[int, float]]:
    return {
        _ANCHORED_LABEL_FOR_SERIES[label]: ice_anchor.apply_multiplier(
            series_hourly[label], multipliers[label]
        )
        for label in multipliers
    }


def _build_one_summary_block(
    target_date: date,
    actuals_hourly: dict[int, float] | None,
    forecast_hourly_by_label: dict[str, dict[int, float]],
    error_label_by_forecast_label: dict[str, str],
) -> pd.DataFrame:
    """Build a single summary block: optional Actual row, the 4 forecast
    rows in OnPeak-asc order, then Error rows in the same order.

    Caller decides which forecast set to pass — raw or ICE-anchored —
    so this helper emits exactly one of the two tables the script prints.
    """
    rows: list[dict] = []
    if actuals_hourly is not None:
        rows.append(_phase1_printers._row(target_date, "Actual", actuals_hourly))

    forecast_rows: list[dict] = []
    for label, hourly in forecast_hourly_by_label.items():
        forecast_rows.append(_phase1_printers._row(target_date, label, hourly))
    forecast_rows.sort(key=_phase1_printers._onpeak_sort_key)
    rows.extend(forecast_rows)

    if actuals_hourly is not None:
        for fr in forecast_rows:
            label = fr["Type"]
            err_label = error_label_by_forecast_label.get(label)
            if err_label is None:
                continue
            rows.append(
                _phase1_printers._error_row(
                    target_date,
                    err_label,
                    forecast_hourly_by_label[label],
                    actuals_hourly,
                )
            )
    return pd.DataFrame(rows, columns=_phase1_printers._OUTPUT_COLS)


def build_summary_tables(
    target_date: date,
    df_forecast: pd.DataFrame,
    actuals_hourly: dict[int, float] | None,
    multipliers: dict[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(scaled_table, raw_table)`` so the runner can print them
    as two separate sections (scaled first, raw second).

    When ``multipliers`` is empty (no ICE anchor available), the scaled
    table is empty and only the raw table renders.
    """
    raw_hourly = _series_hourly_dicts(df_forecast)
    raw_table = _build_one_summary_block(
        target_date,
        actuals_hourly,
        raw_hourly,
        _phase1_printers.ERROR_TYPE_FOR_SERIES,
    )

    if not multipliers:
        empty = pd.DataFrame(columns=_phase1_printers._OUTPUT_COLS)
        return empty, raw_table

    anc_hourly = _anchored_series_hourly(raw_hourly, multipliers)
    scaled_table = _build_one_summary_block(
        target_date,
        actuals_hourly,
        anc_hourly,
        _phase1_printers.ERROR_TYPE_FOR_SERIES,
    )
    return scaled_table, raw_table


_TICKER_META_COLUMNS: tuple[str, ...] = (
    "trade_date",
    "symbol",
    "description",
    "product_type",
    "contract_type",
    "strip",
    "start_date",
    "end_date",
)

_DIRECTION_COLORS: dict[str, str] = (
    {
        "Hit": Colors.BRIGHT_RED,
        "Lift": Colors.BRIGHT_GREEN,
        "Spread": Colors.BRIGHT_YELLOW,
    }
    if _COLOR_ON
    else {}
)


def _color_direction(direction: str, width: int = 6) -> str:
    """Pad ``direction`` to ``width`` then wrap in its ANSI color.

    Mirrors ``modelling/data/pjm_da_next_day_ticker_feed.py::_color_direction``
    so the two scripts render the trade list identically.
    """
    padded = f"{direction:<{width}}"
    color = _DIRECTION_COLORS.get(direction)
    if not color:
        return padded
    return f"{color}{padded}{_RS}"


def print_ice_ticker_feed(
    trades: pd.DataFrame,
    target_date: date,
    symbol: str,
) -> None:
    """Print Meta / Summary / Trades sections for the ICE ticker pull.

    Mirrors the layout of ``pjm_da_next_day_ticker_feed.py`` so the
    anchored pipeline can include the same ticker view inline. ``trades``
    is the frame returned by ``ice_anchor.fetch_ice_ticker_trades`` and
    is expected to carry the columns it pulls (description, strip, etc.).
    """
    print_section("Meta")
    if trades.empty:
        print(
            f"  (no {symbol} trades found for delivery {target_date} — "
            f"ICE feed will be skipped)"
        )
        return

    for col in _TICKER_META_COLUMNS:
        if col not in trades.columns:
            continue
        unique = trades[col].dropna().unique()
        if len(unique) == 1:
            print(f"  {col:<15} {unique[0]}")
        else:
            print(f"  {col:<15} {len(unique)} distinct values")
    print(f"  {'rows':<15} {len(trades)}")

    # Summary uses the most recent trade as 'Last' (matches the live
    # script, which orders DESC and reads .iloc[0]).
    by_time = trades.sort_values("exec_time_local", ascending=False)
    last_price = float(by_time.iloc[0]["price"])
    high = float(trades["price"].max())
    low = float(trades["price"].min())
    volume = float(trades["quantity"].sum())
    print_section("Summary")
    print(f"  {'Last':<15} {last_price:.2f}")
    print(f"  {'High':<15} {high:.2f}")
    print(f"  {'Low':<15} {low:.2f}")
    print(f"  {'Volume':<15} {volume:,.0f}")

    print_section("Trades (most recent first)")
    for _, row in by_time.iterrows():
        direction = _color_direction(str(row["trade_direction"]))
        print(
            f"  {row['exec_time_local']}  "
            f"{direction} "
            f"{float(row['price']):>6.2f}  "
            f"qty {float(row['quantity'])}"
        )


def _print_summary_section(
    title: str,
    table: pd.DataFrame,
    error_types: set[str],
) -> None:
    """Print a single summary block (title + header + rows + footer).

    Mirrors ``_phase1_printers.print_summary_table`` but takes the
    section title and the error-type set as parameters so the anchored
    pipeline can render two distinct blocks (scaled / raw) using the
    matching set of Err labels for gradient coloring.
    """
    print_section(title)
    if table.empty:
        print("  (no rows)")
        return

    _phase1_printers._print_table_header()

    err_rows = table[table["Type"].isin(error_types)]
    err_max_abs = 0.0
    if len(err_rows):
        err_vals = err_rows[_phase1_printers._HE_COLS].to_numpy(dtype=float).ravel()
        err_finite = err_vals[~pd.isna(err_vals)]
        if len(err_finite):
            err_max_abs = float(max(abs(err_finite.min()), abs(err_finite.max())))

    for _, row in table.iterrows():
        is_err = row["Type"] in error_types
        if is_err:
            line = _phase1_printers._format_row_with_gradient(row, err_max_abs)
        else:
            line = _phase1_printers._format_row(row, signed=False)
            style = _phase1_printers._ROW_STYLES.get(row["Type"])
            if style:
                line = f"{style}{line}{_phase1_printers._RS}"
        print(line)

    print("-" * (len(_phase1_printers._HE_COLS) * 7 + 12 + 11 + 7 * 3))


_DIM = Colors.DIM if _COLOR_ON else ""
_HL_HEADER = (Colors.BOLD + Colors.BRIGHT_CYAN) if _COLOR_ON else ""


def print_anchored_config(
    target_date: date,
    hub: str,
    lead_days: int | None,
    det_exec: pd.Timestamp | None,
    ens_exec: pd.Timestamp | None,
    ice_symbol: str,
    vwap_result: ice_anchor.VwapResult | None,
    cutoff: pd.Timestamp | None,
    multipliers: dict[str, float],
    raw_series_hourly: dict[str, dict[int, float]] | None = None,
    shared_scale: float | None = None,
    anchor_label: str | None = None,
    implied_multipliers: dict[str, float] | None = None,
) -> None:
    print_section("Forecast Configuration")
    print(f"  Target           {target_date}")
    print(f"  Hub              {hub}")
    vintage = (
        "DA-cutoff (lead_days=1)"
        if lead_days == 1
        else ("all vintages" if lead_days is None else f"lead_days={lead_days}")
    )
    print(f"  Vintage          {vintage}")
    det_str = det_exec.strftime("%Y-%m-%d %H:%M") if det_exec is not None else "—"
    ens_str = ens_exec.strftime("%Y-%m-%d %H:%M") if ens_exec is not None else "—"
    print(f"  Det executed     {det_str}")
    print(f"  ENS executed     {ens_str}")

    print_section("ICE Anchor")
    print(f"  Symbol           {ice_symbol}")
    cutoff_str = (
        cutoff.strftime("%Y-%m-%d %H:%M")
        if cutoff is not None
        else "(no cutoff — all trades to now)"
    )
    print(f"  Cutoff           {cutoff_str}")
    if vwap_result is None or vwap_result.vwap is None:
        print("  VWAP             — (no eligible trades — anchoring skipped)")
        return
    vr = vwap_result
    print(f"  VWAP             {vr.vwap:>+8,.2f} $/MWh")
    print(f"  Volume           {vr.volume:>8,.0f} MWh   ({vr.n_trades} trades)")
    print(f"  Excluded rows    {vr.n_excluded}  (Leg / null / Spread)")
    last_str = (
        vr.last_time.strftime("%Y-%m-%d %H:%M") if vr.last_time is not None else "—"
    )
    print(f"  Last fill        {vr.last_price:>+8,.2f} @ {last_str}")

    print()
    if shared_scale is None or not multipliers:
        print(
            f"  {_HL_HEADER}Scaling factor{_RS}    "
            "— (could not compute — neither Det nor ENS Avg has usable OnPeak)"
        )
        return

    # ── Highlighted ACTIVE scale ───────────────────────────────────────
    anchor_onpk = (
        ice_anchor.onpeak_mean(raw_series_hourly[anchor_label])
        if raw_series_hourly and anchor_label in (raw_series_hourly or {})
        else None
    )
    anchor_onpk_str = f"{anchor_onpk:+,.2f}" if anchor_onpk is not None else "—"
    print(
        f"  {_HL_HEADER}Active scale     {shared_scale:.4f}{_RS}  "
        f"{_DIM}(anchored to {anchor_label} OnPk = {anchor_onpk_str} → "
        f"ICE VWAP = {vr.vwap:+,.2f}){_RS}"
    )
    print(f"  {_DIM}    formula:  scale = ICE_VWAP / {anchor_label}_OnPk{_RS}")
    print(f"  {_DIM}    apply  :  anchored_HE_h = meteo_HE_h * scale  (h ∈ 1..24){_RS}")
    print(
        f"  {_DIM}    note   :  one shared scale across all 4 series — preserves"
        f" the full ENS dispersion width.{_RS}"
    )

    # ── Per-series implied scales (informational only — NOT applied) ───
    if implied_multipliers:
        print()
        print(f"  {_DIM}Per-series implied scales (informational — NOT applied){_RS}")
        print(
            f"  {_DIM}    these are the scales each Meteo series would use if"
            f" anchored independently;{_RS}"
        )
        print(
            f"  {_DIM}    applying them collapses the ENS band to ICE on OnPk —"
            f" we don't.{_RS}"
        )
        header = (
            f"      {'Series':<12}  {'implied':>9}    "
            f"{'Meteo OnPk':>11}    {'→ ICE OnPk':>11}    {'Δ $/MWh':>9}"
        )
        print(f"  {_DIM}{header}{_RS}")
        for label, implied in implied_multipliers.items():
            meteo_onpk = (
                ice_anchor.onpeak_mean(raw_series_hourly[label])
                if raw_series_hourly and label in raw_series_hourly
                else None
            )
            if meteo_onpk is None:
                line = (
                    f"      {label:<12}  {implied:>9.4f}    "
                    f"{'—':>11}    {vr.vwap:>+11,.2f}    {'—':>9}"
                )
            else:
                delta = vr.vwap - meteo_onpk
                line = (
                    f"      {label:<12}  {implied:>9.4f}    "
                    f"{meteo_onpk:>+11,.2f}    {vr.vwap:>+11,.2f}    {delta:>+9,.2f}"
                )
            print(f"{_DIM}{line}{_RS}")


def run(
    target_date: date | None = TARGET_DATE,
    run_date: date | None = RUN_DATE,
    hub: str = HUB,
    lead_days: int | None = LEAD_DAYS,
    cache_dir: Path | None = CACHE_DIR,
    ice_symbol: str = ICE_SYMBOL,
    ice_vwap_cutoff: pd.Timestamp | None = ICE_VWAP_CUTOFF,
    model_name: str = MODEL_NAME,
    model_family: str = MODEL_FAMILY,
    publish: bool = PUBLISH,
    quiet: bool = False,
) -> dict:
    """Run the ICE-anchored baseline.

    Returns a dict with: ``forecast_date``, ``hub``, ``summary_table``,
    ``members_table``, ``has_actuals``, ``det_forecast_executed``,
    ``ens_forecast_executed``, ``ice_vwap``, ``ice_volume``,
    ``ice_n_trades``, ``ice_multipliers``, ``df_forecast``, ``run_id``.
    ``quiet`` suppresses printing while keeping the return dict full.

    ``publish`` (default ``True``) upserts the run into
    ``pjm_model_outputs.forecast_runs`` via ``publish_forecast_run``;
    batch/backtest callers pass ``publish=False`` to skip the write.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="baseline_meteo_da_price_ice_anchored", log_dir=LOG_DIR)
    try:
        resolved_date = _resolve_target_date(target_date)
        resolved_run_date = run_date if run_date is not None else date.today()
        run_id = str(uuid.uuid4())

        with pl.timer("load Meteologica DA-price forecast"):
            full = loader.load_meteologica_da_price_forecast(
                cache_dir=cache_dir, lead_days=lead_days
            )
        df_forecast = full[full["date"] == resolved_date].copy()

        det_exec = (
            _first_or_none(df_forecast["det_forecast_execution_datetime_local"])
            if "det_forecast_execution_datetime_local" in df_forecast.columns
            else None
        )
        ens_exec = (
            _first_or_none(df_forecast["ens_forecast_execution_datetime_local"])
            if "ens_forecast_execution_datetime_local" in df_forecast.columns
            else None
        )

        actuals_hourly: dict[int, float] | None = None
        if not df_forecast.empty:
            with pl.timer(f"load settled DA LMP at {hub}"):
                lmps = loader.load_lmps_da(cache_dir=cache_dir)
            lmps_at_hub = lmps[lmps["region"].astype(str) == hub]
            actuals_hourly = actuals_from_pool(lmps_at_hub, resolved_date)

        # ICE anchor — single shared scale (Det-anchored, with ENS Avg
        # fallback). Same scale applied to all four series so Meteo's
        # full uncertainty width is preserved. Per-series implied scales
        # are computed for the diagnostic table only.
        vwap_result: ice_anchor.VwapResult | None = None
        multipliers: dict[str, float] = {}
        shared_scale: float | None = None
        anchor_label: str | None = None
        implied_multipliers: dict[str, float] = {}
        trades: pd.DataFrame = pd.DataFrame()
        series_hourly: dict[str, dict[int, float]] = (
            _series_hourly_dicts(df_forecast) if not df_forecast.empty else {}
        )
        if not df_forecast.empty:
            with pl.timer(f"fetch ICE ticker trades for {ice_symbol}"):
                trades = ice_anchor.fetch_ice_ticker_trades(
                    delivery_date=resolved_date,
                    symbol=ice_symbol,
                    cutoff_local=ice_vwap_cutoff,
                )
            vwap_result = ice_anchor.compute_vwap(trades)
            if vwap_result.vwap is not None:
                shared_scale, anchor_label = _compute_shared_multiplier(
                    series_hourly, vwap_result.vwap
                )
                implied_multipliers = _implied_per_series_multipliers(
                    series_hourly, vwap_result.vwap
                )
                if shared_scale is None:
                    pl.warning(
                        "ICE VWAP available but neither Det nor ENS Avg has a "
                        "usable OnPeak mean — anchoring skipped."
                    )
                else:
                    multipliers = {label: shared_scale for label in series_hourly}
            else:
                pl.warning(
                    f"No eligible ICE trades for {ice_symbol} on {resolved_date} "
                    f"(rows={len(trades)}, excluded={vwap_result.n_excluded}). "
                    f"Anchoring skipped — falling back to raw Meteo."
                )

        # Build the dataframes we'll feed into the new three-section layout.
        # ``df_for_fan`` carries the scaled prices when anchoring is active —
        # it backs the verification sections (Forecast vs Actuals + Bands
        # vs Actuals) so they reflect the trader's actual published view.
        if shared_scale is not None and not df_forecast.empty:
            df_for_fan = _scale_forecast_df(df_forecast, shared_scale)
        else:
            df_for_fan = df_forecast

        bands_table_raw = build_bands_table(resolved_date, df_forecast)
        bands_table_scaled = (
            build_bands_table(resolved_date, df_for_fan)
            if shared_scale is not None
            else pd.DataFrame()
        )
        forecast_vs_actuals = build_forecast_vs_actuals(
            resolved_date, df_for_fan, actuals_hourly
        )
        bands_vs_actuals = build_bands_vs_actuals(
            resolved_date, df_for_fan, actuals_hourly
        )
        members_table = build_members_table(resolved_date, df_for_fan)
        dispersion_raw = (
            compute_dispersion_metrics(df_forecast) if not df_forecast.empty else None
        )
        dispersion_scaled = (
            compute_dispersion_metrics(df_for_fan) if not df_forecast.empty else None
        )
        # ``dispersion`` retained for backward-compat: callers expecting
        # one value get the scaled one (anchored view).
        dispersion = dispersion_scaled

        if publish and not df_forecast.empty:
            with pl.timer(f"publish ICE-anchored forecast JSON ({model_name})"):
                from backend.modelling.da_models.baseline_meteo_da_price.publish import (  # noqa: PLC0415
                    build_payload,
                    extract_onpeak_forecast,
                )

                payload = build_payload(
                    df_for_fan=df_for_fan,
                    bands_table_scaled=bands_table_scaled,
                    bands_table_raw=bands_table_raw,
                    actuals_hourly=actuals_hourly,
                    trades=trades,
                    vwap_result=vwap_result,
                    target_date=resolved_date,
                    run_date=resolved_run_date,
                    model_name=model_name,
                    model_family=model_family,
                    run_id=run_id,
                    hub=hub,
                    lead_days=lead_days,
                    det_exec=det_exec,
                    ens_exec=ens_exec,
                    ice_symbol=ice_symbol,
                    ice_cutoff=ice_vwap_cutoff,
                    shared_scale=shared_scale,
                    anchor_label=anchor_label,
                    implied_multipliers=implied_multipliers,
                )
                publish_forecast_run(
                    model_name=model_name,
                    model_family=model_family,
                    target_date=resolved_date,
                    run_date=resolved_run_date,
                    run_id=run_id,
                    payload=payload,
                    da_lmp_total_onpeak_forecast=extract_onpeak_forecast(payload),
                )

        if not quiet:
            print_header(
                f"BASELINE METEO DA-PRICE (ICE ANCHORED) — {hub} ($/MWh)  |  {resolved_date}",
                "=",
                120,
            )

            # 1) ICE ticker feed (Meta / Summary / Trades) — first, so the
            # trader sees the market context before any forecast tables.
            print_ice_ticker_feed(trades, resolved_date, ice_symbol)

            # 2) Forecast / anchor configuration block.
            print_anchored_config(
                resolved_date,
                hub,
                lead_days,
                det_exec,
                ens_exec,
                ice_symbol,
                vwap_result,
                ice_vwap_cutoff,
                multipliers,
                raw_series_hourly=series_hourly,
                shared_scale=shared_scale,
                anchor_label=anchor_label,
                implied_multipliers=implied_multipliers,
            )

            if df_forecast.empty:
                pl.warning(
                    f"No Meteologica DA-price forecast for {resolved_date} "
                    f"(lead_days={lead_days}). Tables are empty."
                )
            else:
                pl.info(
                    f"forecast rows: {len(df_forecast)} | "
                    f"actuals: {'yes' if actuals_hourly else 'no'} | "
                    f"anchored: {'yes' if multipliers else 'no'}"
                )

            # 3) ENS Bands — unscaled first, then scaled so the user can
            # compare the ICE-anchored levels against the raw Meteo levels.
            print_bands_section(
                resolved_date,
                bands_table_raw,
                dispersion_raw,
                title="ENS Bands — Unscaled (raw Meteo) ($/MWh)",
            )
            if shared_scale is not None and not bands_table_scaled.empty:
                print_bands_section(
                    resolved_date,
                    bands_table_scaled,
                    dispersion_scaled,
                    title="ENS Bands — Scaled to ICE ($/MWh)",
                )

            # 4) Verification sections — only when settled DA LMP exists.
            #    Both consume the SCALED df so values match the scaled
            #    bands above (Det as the central forecast).
            if actuals_hourly is not None:
                print_forecast_vs_actuals_section(resolved_date, forecast_vs_actuals)
                print_bands_vs_actuals_section(resolved_date, bands_vs_actuals)

            print()
            print_divider("=", 120, dim=False)
            print()

        return {
            "forecast_date": str(resolved_date),
            "hub": hub,
            "bands_table_raw": bands_table_raw,
            "bands_table_scaled": bands_table_scaled,
            "forecast_vs_actuals": forecast_vs_actuals,
            "bands_vs_actuals": bands_vs_actuals,
            "members_table": members_table,
            "dispersion_metrics": dispersion,
            "dispersion_metrics_raw": dispersion_raw,
            "has_actuals": actuals_hourly is not None,
            "det_forecast_executed": det_exec,
            "ens_forecast_executed": ens_exec,
            "ice_vwap": vwap_result.vwap if vwap_result else None,
            "ice_volume": vwap_result.volume if vwap_result else 0.0,
            "ice_n_trades": vwap_result.n_trades if vwap_result else 0,
            "ice_multipliers": multipliers,
            "ice_shared_scale": shared_scale,
            "ice_anchor_label": anchor_label,
            "ice_implied_multipliers": implied_multipliers,
            "ice_trades": trades,
            "df_forecast": df_forecast,
            "run_id": run_id,
        }
    finally:
        pl.close()


if __name__ == "__main__":
    run()
