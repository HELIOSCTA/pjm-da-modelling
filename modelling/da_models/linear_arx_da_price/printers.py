"""Terminal printers for the linear ARX DA-price forecast.

Deliberately mirrors the layout of
``da_models.like_day_model_knn.pjm_rto_hourly.printers`` so a side-by-side
run of the two single-day pipelines produces visually-comparable output:
same FORECAST CONFIGURATION banner, same ``Quantile Bands`` / ``Forecast
vs Actuals`` / ``Quantile Bands vs Actuals`` sub-sections, same per-HE
gradient coloring (dark green = good, dark red = bad). Kept local to the
family -- no cross-family import -- per the modelling/CLAUDE.md rule.

Where the like-day report shows analog-specific sections (ANALOG DAYS,
ANALOG FEATURES), this one shows a ``MODEL DIAGNOSTICS`` section instead
(per-hour ridge alpha / training rows, top feature coefficients, the
backward-vs-forward coefficient-mass share) -- the regression analogue of
"why this forecast".
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from da_models.common.evaluation.metrics import (
    coverage,
    crps_from_quantiles,
    point_errors,
)
from da_models.linear_arx_da_price import configs as C
from da_models.linear_arx_da_price.forecast import quantile_label
from utils.logging_utils import (
    Colors,
    print_divider,
    print_header,
    print_section,
    supports_color,
)

_COLOR_ON = supports_color()
_DARK_ORANGE_256 = "\033[38;5;166m"
_PURPLE_256 = "\033[38;5;93m"
_DARK_GREEN_256 = "\033[38;5;22m"
_GREEN_256 = "\033[38;5;28m"
_RED_256 = "\033[38;5;124m"
_DARK_RED_256 = "\033[38;5;88m"

_HL_FORECAST = (Colors.BOLD + Colors.BRIGHT_BLUE) if _COLOR_ON else ""
_HL_ACTUAL = (Colors.BOLD + _PURPLE_256) if _COLOR_ON else ""
_HL_QUARTILE = (Colors.BOLD + Colors.YELLOW) if _COLOR_ON else ""
_HL_OUTER_QUANTILE = (Colors.BOLD + _DARK_ORANGE_256) if _COLOR_ON else ""
_HL_UP = Colors.BRIGHT_GREEN if _COLOR_ON else ""
_HL_DOWN = Colors.BRIGHT_RED if _COLOR_ON else ""
_HL_NOTE = Colors.DIM if _COLOR_ON else ""
_RS = Colors.RESET if _COLOR_ON else ""

_ROW_STYLES: dict[str, str] = {
    "Actual": _HL_ACTUAL,
    "Forecast": _HL_FORECAST,
    "P50": _HL_FORECAST,
    "P25": _HL_QUARTILE,
    "P75": _HL_QUARTILE,
    "P10": _HL_OUTER_QUANTILE,
    "P90": _HL_OUTER_QUANTILE,
}

_DAY_ABBR = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_HE_COLS = [f"HE{h}" for h in range(1, 25)]
_BLOCK_HOUR_INDICES: tuple[tuple[str, list[int]], ...] = (
    ("OnPeak", list(range(7, 23))),
    ("OffPeak", list(range(0, 7)) + [23]),
    ("Flat", list(range(24))),
)


# ── Gradient coloring (lifted from the like-day printer) ───────────────────
def _gradient_color(val: float, lo: float, hi: float, kind: str) -> str:
    if not _COLOR_ON or not np.isfinite(val):
        return ""
    if kind == "abs_low_is_good":
        max_abs = max(abs(lo), abs(hi))
        if max_abs <= 0:
            return ""
        norm = abs(val) / max_abs
    else:
        if hi <= lo:
            return ""
        norm = (val - lo) / (hi - lo)
    if norm < 0.10:
        return _DARK_GREEN_256
    if norm < 0.20:
        return _GREEN_256
    if norm < 0.35:
        return ""
    if norm < 0.65:
        return _RED_256
    return _DARK_RED_256


def _wrap_gradient(raw_cell: str, val: float, lo: float, hi: float, kind: str) -> str:
    color = _gradient_color(val, lo, hi, kind)
    if not color:
        return raw_cell
    stripped = raw_cell.lstrip(" ")
    leading = raw_cell[: len(raw_cell) - len(stripped)]
    return f"{leading}{color}{stripped}{_RS}"


def _band_header() -> str:
    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    return header


def _print_band_rows(table: pd.DataFrame, types_order: list[str] | None = None) -> None:
    rows = (
        table
        if types_order is None
        else pd.concat(
            [
                table[table["Type"] == t]
                for t in types_order
                if (table["Type"] == t).any()
            ]
        )
    )
    for _, row in rows.iterrows():
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row.get(f"HE{h}")
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        for col in ("OnPeak", "OffPeak", "Flat"):
            val = row.get(col)
            line += f" {val:>7.2f}" if pd.notna(val) else f" {'':>7}"
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        print(line)


def _render_per_hour_metric_row(
    date_str: str,
    label: str,
    values: np.ndarray,
    fmt: str,
    summary_fmt: str,
    gradient_kind: str | None = None,
) -> None:
    finite = values[np.isfinite(values)] if len(values) else np.array([])
    lo = float(finite.min()) if len(finite) else 0.0
    hi = float(finite.max()) if len(finite) else 0.0
    line = f"{date_str:<12} {label:<10}"
    for h_idx in range(24):
        v = values[h_idx] if h_idx < len(values) else np.nan
        if np.isfinite(v):
            cell = f" {v:>{fmt}}"
            if gradient_kind:
                cell = _wrap_gradient(cell, float(v), lo, hi, gradient_kind)
            line += cell
        else:
            line += f" {'':>6}"
    for _, idx in _BLOCK_HOUR_INDICES:
        block_vals = [
            values[i] for i in idx if i < len(values) and np.isfinite(values[i])
        ]
        if block_vals:
            mean = float(np.mean(block_vals))
            cell = f" {mean:>{summary_fmt}}"
            if gradient_kind:
                cell = _wrap_gradient(cell, mean, lo, hi, gradient_kind)
            line += cell
        else:
            line += f" {'':>7}"
    print(line)


def _render_inband_row(date_str: str, in_band_80: list[bool | None]) -> None:
    line = f"{date_str:<12} {'InBand 80%':<10}"
    for h_idx in range(24):
        b = in_band_80[h_idx] if h_idx < len(in_band_80) else None
        if b is None:
            line += f" {'':>6}"
            continue
        mark = "Y" if b else "N"
        color = (_HL_UP if b else _HL_DOWN) if _COLOR_ON else ""
        line += f" {color}{mark:>6}{_RS}" if color else f" {mark:>6}"
    for _, idx in _BLOCK_HOUR_INDICES:
        valid = [
            in_band_80[i]
            for i in idx
            if i < len(in_band_80) and in_band_80[i] is not None
        ]
        if valid:
            pct = sum(1 for b in valid if b) / len(valid) * 100.0
            line += f" {f'{pct:.0f}%':>7}"
        else:
            line += f" {'':>7}"
    print(line)


# ── Section: FORECAST CONFIGURATION ────────────────────────────────────────
def print_config(
    target_date: date,
    hub: str,
    feature_cols: list[str],
    dropped_groups: list[str],
    variant_cfg,
) -> None:
    target_dow = _DAY_ABBR[target_date.weekday()]
    win_start = target_date - timedelta(days=C.TRAIN_WINDOW_DAYS)
    print_header("FORECAST CONFIGURATION", "=", 120)
    print(f"\n  Target        {target_date} ({target_dow})")
    print(f"  Hub           {hub}")
    print(f"  Variant       {variant_cfg.VARIANT}")
    print(f"  Model         {variant_cfg.MODEL_NAME}  (family: {C.MODEL_FAMILY})")
    print(f"  Description   {variant_cfg.DESCRIPTION}")

    print_section("Estimator")
    print("  Type               Ridge (L2), one model per hour-ending")
    print(
        f"  alpha grid         {C.RIDGE_ALPHAS}  (selected by {C.CV_SPLITS}-fold expanding-window TS-CV)"
    )
    print(
        "  Target transform   asinh (variance-stabilizing) -- predict in asinh space, sinh back"
    )
    print(
        f"  Bands              residual-quantile (last {C.RESIDUAL_HOLDOUT_DAYS}d in-sample residuals, asinh space)"
    )
    print(
        f"  Display quantiles  {', '.join(quantile_label(q) for q in C.DISPLAY_QUANTILES)}"
    )

    print_section("Calibration Window")
    print(
        f"  Train window       {C.TRAIN_WINDOW_DAYS}d  "
        f"({win_start.strftime('%b %d %Y')} - {target_date.strftime('%b %d %Y')})"
    )
    print(
        f"  Recency weighting  gamma={C.RECENCY_GAMMA:.5f}  (half-life {C.RECENCY_HALFLIFE_DAYS}d)"
    )
    print(f"  Min rows / hour    {C.MIN_TRAIN_ROWS_PER_HOUR}")

    print_section("Features")
    print(f"  Feature count      {len(feature_cols)}")
    print(f"  Demand block       {variant_cfg.DEMAND_BLOCK_LABEL}")
    print(
        "  Shared feeds       weather, ICE next-day gas, PJM outage forecast, calendar"
    )
    print("  Engineered         load_x_gas, load_sq, outage_sq")
    bwd = "on" if variant_cfg.INCLUDE_BACKWARD_LMP else "off"
    print(
        f"  Backward LMP       {bwd}  (reference day D-{variant_cfg.BACKWARD_LMP_DEFAULT_LAG_DAYS}, "
        f"Mondays D-{variant_cfg.BACKWARD_LMP_MONDAY_LAG_DAYS})"
    )
    if dropped_groups:
        print(
            f"  Dropped groups     {', '.join(dropped_groups)}  (parquet unavailable)"
        )

    _print_config_modules(variant_cfg)
    print()
    print_divider("=", 120, dim=False)


def _fmt_const(v: object) -> str:
    if isinstance(v, float):
        return f"{v:.6g}"
    if isinstance(v, tuple):
        return "(" + ", ".join(_fmt_const(x) for x in v) + ")"
    return str(v)


def _module_constants(mod) -> list[tuple[str, object]]:
    """Public ALL-CAPS module-level constants, in definition order."""
    return [
        (name, val)
        for name, val in vars(mod).items()
        if name.isupper() and not name.startswith("_")
    ]


def _print_config_modules(variant_cfg) -> None:
    """Echo both config modules in full -- shared estimator/window/band
    constants (``configs.py``) and the variant's feature-source knobs
    (``<variant>/config.py``) -- with their file paths, so a reader knows
    exactly which file to edit for which knob."""
    print_section("Config Modules")
    for label, mod in (("shared ", C), ("variant", variant_cfg)):
        path = getattr(mod, "__file__", "?")
        print(f"  {label}  {mod.__name__}")
        print(f"           {path}")
        for name, val in _module_constants(mod):
            print(f"             {name} = {_fmt_const(val)}")


# ── Section: MODEL DIAGNOSTICS (regression analogue of the analog tables) ──
def print_model_diagnostics(models, feature_cols: list[str]) -> None:
    print_section("Model Diagnostics")
    if not models.by_hour:
        print("  (no hours trained -- insufficient data)")
        return

    # Per-hour alpha / training rows.
    header = f"  {'HE':>3} {'alpha':>9} {'n_train':>8}    "
    print(header + f"{'HE':>3} {'alpha':>9} {'n_train':>8}")
    print("  " + "-" * (len(header) + 22))
    hours = list(C.HOURS)
    half = (len(hours) + 1) // 2
    for i in range(half):
        left = models.by_hour.get(hours[i])
        right = models.by_hour.get(hours[i + half]) if i + half < len(hours) else None
        lstr = (
            f"  {hours[i]:>3} {left.alpha:>9.2f} {left.n_train:>8}"
            if left
            else f"  {hours[i]:>3} {'(skip)':>9} {'':>8}"
        )
        rstr = ""
        if i + half < len(hours):
            rstr = (
                f"    {hours[i + half]:>3} {right.alpha:>9.2f} {right.n_train:>8}"
                if right
                else f"    {hours[i + half]:>3} {'(skip)':>9} {'':>8}"
            )
        print(lstr + rstr)

    # Top feature coefficients, averaged |coef| across hours (standardized space).
    if models.by_hour:
        mat = np.vstack([m.coef for m in models.by_hour.values()])
        mean_abs = np.abs(mat).mean(axis=0)
        total = mean_abs.sum()
        order = np.argsort(mean_abs)[::-1][:12]
        print()
        print(
            f"  Top feature coefficients (mean |coef| across hours, standardized; "
            f"backward-LMP share = {models.backward_coef_share:.3f})"
        )
        for j in order:
            share = mean_abs[j] / total if total > 0 else 0.0
            bar = "#" * int(round(share * 60))
            tag = "  <- backward" if feature_cols[j].startswith("bwd_lmp_") else ""
            print(
                f"    {feature_cols[j]:<24} {mean_abs[j]:>8.4f}  {share:>6.1%}  {bar}{tag}"
            )
    if not np.isnan(models.backward_coef_share) and models.backward_coef_share > 0.60:
        print(
            "  WARNING: backward-looking LMP features dominate (>0.60) -- forecast may anchor to recent prices"
        )
    print()


# ── Section: Quantile Bands ($/MWh) ────────────────────────────────────────
def print_quantiles(table: pd.DataFrame) -> None:
    print_section("Quantile Bands ($/MWh)")
    print(
        f"  {_HL_NOTE}Per-HE bands: ridge point forecast (asinh space) plus the empirical"
        f" residual quantile, transformed back with sinh.{_RS}"
    )
    print(
        f"  {_HL_NOTE}OnPeak / OffPeak / Flat: simple means of the per-HE band over the"
        f" block hours.{_RS}"
    )
    print()
    header = _band_header()
    print(header)
    print("-" * len(header))
    _print_band_rows(table)

    p10 = table[table["Type"] == "P10"]
    p25 = table[table["Type"] == "P25"]
    p50 = table[table["Type"] == "P50"]
    p75 = table[table["Type"] == "P75"]
    p90 = table[table["Type"] == "P90"]

    def _arr(rows: pd.DataFrame) -> np.ndarray:
        if not len(rows):
            return np.full(24, np.nan)
        r = rows.iloc[0]
        return np.array([r.get(f"HE{h}") for h in range(1, 25)], dtype=float)

    if len(p10) and len(p90):
        date_str = str(p10.iloc[0]["Date"])
        a10, a25, a50, a75, a90 = _arr(p10), _arr(p25), _arr(p50), _arr(p75), _arr(p90)
        print("-" * len(header))
        _render_per_hour_metric_row(
            date_str, "Width", a90 - a10, "6.2f", "7.2f", "low_is_good"
        )
        if np.isfinite(a25).any() and np.isfinite(a75).any():
            _render_per_hour_metric_row(
                date_str, "IQR", a75 - a25, "6.2f", "7.2f", "low_is_good"
            )
        if np.isfinite(a50).any():
            skew = (a90 - a50) - (a50 - a10)
            _render_per_hour_metric_row(
                date_str, "Skew", skew, "+6.2f", "+7.2f", "abs_low_is_good"
            )
            d_p50 = np.full(24, np.nan)
            d_p50[1:] = np.diff(a50)
            _render_per_hour_metric_row(
                date_str, "d P50", d_p50, "+6.2f", "+7.2f", "abs_low_is_good"
            )
    print("-" * len(header))


# ── Section: Forecast vs Actuals ───────────────────────────────────────────
def print_forecast(table: pd.DataFrame, block_level: dict | None = None) -> None:
    print_section("Forecast vs Actuals")
    header = _band_header()
    print(header)
    print("-" * len(header))

    error_rows = table[table["Type"] == "Error"]
    err_lo, err_hi = 0.0, 0.0
    if len(error_rows):
        err_arr = np.array(
            [error_rows.iloc[0].get(f"HE{h}") for h in range(1, 25)], dtype=float
        )
        finite = err_arr[np.isfinite(err_arr)]
        if len(finite):
            err_lo, err_hi = float(finite.min()), float(finite.max())

    for _, row in table.iterrows():
        is_error = row["Type"] == "Error"
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row.get(f"HE{h}")
            if pd.notna(val):
                cell = f" {val:>6.1f}"
                if is_error:
                    cell = _wrap_gradient(
                        cell, float(val), err_lo, err_hi, "abs_low_is_good"
                    )
                line += cell
            else:
                line += f" {'':>6}"
        for col in ("OnPeak", "OffPeak", "Flat"):
            val = row.get(col)
            if pd.notna(val):
                cell = f" {val:>7.2f}"
                if is_error:
                    cell = _wrap_gradient(
                        cell, float(val), err_lo, err_hi, "abs_low_is_good"
                    )
                line += cell
            else:
                line += f" {'':>7}"
        if not is_error:
            style = _ROW_STYLES.get(row["Type"])
            if style:
                line = f"{style}{line}{_RS}"
        print(line)

    actual_rows = table[table["Type"] == "Actual"]
    forecast_rows = table[table["Type"] == "Forecast"]
    if len(actual_rows) and len(forecast_rows):
        a_row, f_row = actual_rows.iloc[0], forecast_rows.iloc[0]
        date_str = str(a_row["Date"])
        actual_arr = np.array([a_row.get(f"HE{h}") for h in range(1, 25)], dtype=float)
        forecast_arr = np.array(
            [f_row.get(f"HE{h}") for h in range(1, 25)], dtype=float
        )
        abs_err = np.abs(forecast_arr - actual_arr)
        with np.errstate(invalid="ignore", divide="ignore"):
            mape_pct = np.where(
                np.abs(actual_arr) > 1e-9, abs_err / np.abs(actual_arr) * 100.0, np.nan
            )
        ae_f = abs_err[np.isfinite(abs_err)]
        ae_lo, ae_hi = (
            (float(ae_f.min()), float(ae_f.max())) if len(ae_f) else (0.0, 0.0)
        )
        mp_f = mape_pct[np.isfinite(mape_pct)]
        mp_lo, mp_hi = (
            (float(mp_f.min()), float(mp_f.max())) if len(mp_f) else (0.0, 0.0)
        )

        def _summary(blk: str, key: str) -> str:
            if not block_level:
                return f" {'':>7}"
            v = block_level.get(blk, {}).get(key)
            if v is None or not np.isfinite(v):
                return f" {'':>7}"
            cell = f" {v:>6.1f}%" if key == "mape" else f" {v:>7.2f}"
            if key == "mae":
                return _wrap_gradient(cell, float(v), ae_lo, ae_hi, "low_is_good")
            if key == "mape":
                return _wrap_gradient(cell, float(v), mp_lo, mp_hi, "low_is_good")
            return cell

        line = f"{date_str:<12} {'|Err|':<10}"
        for h_idx in range(24):
            v = abs_err[h_idx]
            if np.isfinite(v):
                line += _wrap_gradient(
                    f" {v:>6.1f}", float(v), ae_lo, ae_hi, "low_is_good"
                )
            else:
                line += f" {'':>6}"
        for blk in ("OnPeak", "OffPeak", "Flat"):
            line += _summary(blk, "mae")
        print(line)

        line = f"{date_str:<12} {'MAPE %':<10}"
        for h_idx in range(24):
            v = mape_pct[h_idx]
            if np.isfinite(v):
                line += _wrap_gradient(
                    f" {v:>5.1f}%", float(v), mp_lo, mp_hi, "low_is_good"
                )
            else:
                line += f" {'':>6}"
        for blk in ("OnPeak", "OffPeak", "Flat"):
            line += _summary(blk, "mape")
        print(line)

    print("-" * len(header))
    if block_level:
        parts = []
        for blk in ("OnPeak", "OffPeak", "Flat"):
            v = block_level.get(blk, {}).get("rmse")
            if v is not None and np.isfinite(v):
                parts.append(f"{blk}={v:.2f}")
        if parts:
            print(f"  RMSE:  {'   '.join(parts)}")
        rmae_parts = []
        for blk in ("OnPeak", "OffPeak", "Flat"):
            v = block_level.get(blk, {}).get("rmae")
            if v is not None and np.isfinite(v):
                rmae_parts.append(f"{blk}={v:.3f}")
        if rmae_parts:
            print(f"  rMAE vs d-7:  {'   '.join(rmae_parts)}")


# ── Section: Quantile Bands vs Actuals ─────────────────────────────────────
def print_band_calibration(
    output_table: pd.DataFrame,
    quantiles_table: pd.DataFrame,
    in_band_80: list[bool | None] | None = None,
    crps_per_hour: np.ndarray | None = None,
) -> None:
    if quantiles_table is None or len(quantiles_table) == 0:
        return
    p10 = quantiles_table[quantiles_table["Type"] == "P10"]
    p90 = quantiles_table[quantiles_table["Type"] == "P90"]
    actual = output_table[output_table["Type"] == "Actual"]
    if not len(p10) or not len(p90) or not len(actual):
        return

    print_section("Quantile Bands vs Actuals")
    header = _band_header()
    print(header)
    print("-" * len(header))
    for row in (p10.iloc[0], actual.iloc[0], p90.iloc[0]):
        line = f"{str(row['Date']):<12} {row['Type']:<10}"
        for h in range(1, 25):
            val = row.get(f"HE{h}")
            line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
        for col in ("OnPeak", "OffPeak", "Flat"):
            val = row.get(col)
            line += f" {val:>7.2f}" if pd.notna(val) else f" {'':>7}"
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        print(line)

    date_str = str(actual.iloc[0]["Date"])
    if in_band_80 is not None and any(b is not None for b in in_band_80):
        _render_inband_row(date_str, in_band_80)
    if crps_per_hour is not None and np.isfinite(crps_per_hour).any():
        _render_per_hour_metric_row(
            date_str, "CRPS", crps_per_hour, "6.3f", "7.3f", "low_is_good"
        )
    print("-" * len(header))


# ── Metric helpers (consumed by the pipeline) ──────────────────────────────
_BLOCK_INDICES: dict[str, np.ndarray] = {
    "OnPeak": np.array(range(7, 23), dtype=int),
    "OffPeak": np.array(list(range(0, 7)) + [23], dtype=int),
    "Flat": np.array(range(0, 24), dtype=int),
}


def compute_block_level(
    actual_arr: np.ndarray, forecast_arr: np.ndarray, naive_full: np.ndarray | None
) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for name, idx in _BLOCK_INDICES.items():
        a, f = actual_arr[idx], forecast_arr[idx]
        nan_row = {k: float("nan") for k in ("mae", "rmse", "mape", "rmae")}
        if np.isnan(a).any() or np.isnan(f).any():
            out[name] = nan_row
            continue
        err = f - a
        mae_ = float(np.mean(np.abs(err)))
        rmse_ = float(np.sqrt(np.mean(err**2)))
        with np.errstate(divide="ignore", invalid="ignore"):
            mape_arr = np.abs(err) / np.where(a == 0, np.nan, np.abs(a))
        mape_ = (
            float(np.nanmean(mape_arr) * 100.0)
            if np.isfinite(mape_arr).any()
            else float("nan")
        )
        rmae_ = float("nan")
        if naive_full is not None and not np.isnan(naive_full[idx]).any():
            nm = float(np.mean(np.abs(naive_full[idx] - a)))
            if nm > 0:
                rmae_ = mae_ / nm
        out[name] = {"mae": mae_, "rmse": rmse_, "mape": mape_, "rmae": rmae_}
    return out


def compute_in_band_80(
    quantiles_table: pd.DataFrame, actuals_hourly: dict[int, float] | None
) -> list[bool | None]:
    out: list[bool | None] = []
    if actuals_hourly is None or quantiles_table is None or len(quantiles_table) == 0:
        return out
    p10 = quantiles_table[quantiles_table["Type"] == "P10"]
    p90 = quantiles_table[quantiles_table["Type"] == "P90"]
    if not len(p10) or not len(p90):
        return out
    r10, r90 = p10.iloc[0], p90.iloc[0]
    for h in range(1, 25):
        a = actuals_hourly.get(h)
        lo, hi = r10.get(f"HE{h}"), r90.get(f"HE{h}")
        if a is None or pd.isna(lo) or pd.isna(hi):
            out.append(None)
        else:
            out.append(bool(lo <= a <= hi))
    return out


def compute_crps_per_hour(
    df_forecast: pd.DataFrame, actuals_hourly: dict[int, float] | None
) -> np.ndarray:
    crps = np.full(24, np.nan)
    if actuals_hourly is None or df_forecast is None or len(df_forecast) == 0:
        return crps
    for _, r in df_forecast.iterrows():
        h = int(r["hour_ending"])
        a = actuals_hourly.get(h)
        if a is None:
            continue
        per_q = []
        for q in C.QUANTILES:
            col = f"q_{q:.2f}"
            if col in df_forecast.columns and pd.notna(r.get(col)):
                e = float(a) - float(r[col])
                per_q.append(max(q * e, (q - 1.0) * e))
        if per_q:
            crps[h - 1] = 2.0 * float(np.mean(per_q))
    return crps


def compute_metrics(
    fc: pd.DataFrame,
    actuals_hourly: dict[int, float] | None,
    naive_d7: dict[int, float] | None,
) -> dict:
    """Scalar headline metrics for the return dict (the printed report uses
    the per-block helpers above)."""
    out: dict[str, float] = {}
    if actuals_hourly is None:
        return out
    hours = [h for h in range(1, 25) if h in actuals_hourly]
    by_he = {int(r["hour_ending"]): r for _, r in fc.iterrows()}
    y = np.array([actuals_hourly[h] for h in hours], dtype=float)
    f = np.array(
        [by_he.get(h, {}).get("point_forecast", np.nan) for h in hours], dtype=float
    )
    mask = ~np.isnan(f)
    if mask.sum() == 0:
        return out
    y_m, f_m = y[mask], f[mask]
    out.update(point_errors(y_m, f_m))
    if naive_d7 is not None:
        n = np.array([naive_d7.get(h, np.nan) for h in hours], dtype=float)[mask]
        nm = ~np.isnan(n)
        if nm.sum() > 0 and np.mean(np.abs(y_m[nm] - n[nm])) > 0:
            out["rmae_vs_d7"] = float(
                np.mean(np.abs(y_m[nm] - f_m[nm])) / np.mean(np.abs(y_m[nm] - n[nm]))
            )
    qpreds: dict[float, np.ndarray] = {}
    for q in C.QUANTILES:
        col = f"q_{q:.2f}"
        if col in fc.columns:
            arr = np.array(
                [by_he.get(h, {}).get(col, np.nan) for h in hours], dtype=float
            )[mask]
            if not np.isnan(arr).any():
                qpreds[q] = arr
    if qpreds:
        out["pinball_crps"] = crps_from_quantiles(y_m, qpreds)
    lo_col, hi_col = f"q_{min(C.QUANTILES):.2f}", f"q_{max(C.QUANTILES):.2f}"
    if lo_col in fc.columns and hi_col in fc.columns:
        lo = np.array(
            [by_he.get(h, {}).get(lo_col, np.nan) for h in hours], dtype=float
        )[mask]
        hi = np.array(
            [by_he.get(h, {}).get(hi_col, np.nan) for h in hours], dtype=float
        )[mask]
        if not (np.isnan(lo).any() or np.isnan(hi).any()):
            out["coverage_p10_p90"] = coverage(y_m, lo, hi)
    on_h = [h for h in range(8, 24) if h in actuals_hourly]
    fa = np.array(
        [by_he.get(h, {}).get("point_forecast", np.nan) for h in on_h], dtype=float
    )
    ya = np.array([actuals_hourly[h] for h in on_h], dtype=float)
    if len(on_h) > 0 and not np.isnan(fa).any():
        out["onpeak_block_mean_err"] = float(np.mean(fa) - np.mean(ya))
    return out
