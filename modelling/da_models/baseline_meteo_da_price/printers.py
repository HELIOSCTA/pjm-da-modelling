"""Terminal printers for the Meteologica DA-price baseline.

Mirrors the visual style of
``da_models.like_day_model_knn.pjm_rto_hourly.printers.print_forecast``
(``Date | Type | HE1..HE24 | OnPk | OffPk | Flat`` with per-row ANSI
colors gated on ``supports_color()``) but kept local — no cross-family
import — per the family-import rule in ``modelling/CLAUDE.md``.

Two builders + their printers:

* ``build_summary_table`` / ``print_summary_table`` — the four named
  Meteologica series (deterministic + ENS avg/bottom/top), with
  Actual + per-series Error rows when settled DA LMP exists.
* ``build_members_table`` / ``print_members_table`` — ENS Bottom row,
  the 51 ECMWF members ranked by OnPeak ascending, then ENS Top row.
  The bracketing rows wear the bottom/top colors; the 51 members in
  between are dim so the eye lands on the floor / ceiling.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from da_models.common.forecast.output import add_summary_cols
from utils.logging_utils import Colors, print_section, supports_color

_COLOR_ON = supports_color()

# 256-color escapes for shades the base palette doesn't expose.
_DARK_GREEN_256 = "\033[38;5;22m"
_GREEN_256 = "\033[38;5;28m"
_RED_256 = "\033[38;5;124m"
_DARK_RED_256 = "\033[38;5;88m"
_DARK_ORANGE_256 = "\033[38;5;166m"
_PURPLE_256 = "\033[38;5;93m"

_RS = Colors.RESET if _COLOR_ON else ""
_DIM = Colors.DIM if _COLOR_ON else ""

SERIES_TYPES: tuple[str, ...] = ("Det", "ENS Avg", "ENS Bottom", "ENS Top")
SERIES_TO_COL: dict[str, str] = {
    "Det": "da_price_deterministic",
    "ENS Avg": "da_price_ens_average",
    "ENS Bottom": "da_price_ens_bottom",
    "ENS Top": "da_price_ens_top",
}
ERROR_TYPE_FOR_SERIES: dict[str, str] = {
    "Det": "Err Det",
    "ENS Avg": "Err Avg",
    "ENS Bottom": "Err Bot",
    "ENS Top": "Err Top",
}

# Series -> row style (whole-line color). Both tables share this dict.
# ENS Bottom and ENS Top share the "envelope" color (dark orange) — they
# are the floor/ceiling pair and read as one band, mirroring the
# _HL_OUTER_QUANTILE convention in like_day_model_knn.
_ENS_ENVELOPE_STYLE = (Colors.BOLD + _DARK_ORANGE_256) if _COLOR_ON else ""
_ROW_STYLES: dict[str, str] = {
    "Actual": (Colors.BOLD + _PURPLE_256) if _COLOR_ON else "",
    "Det": (Colors.BOLD + Colors.BRIGHT_BLUE) if _COLOR_ON else "",
    "ENS Avg": (Colors.BOLD + Colors.YELLOW) if _COLOR_ON else "",
    "ENS Bottom": _ENS_ENVELOPE_STYLE,
    "ENS Top": _ENS_ENVELOPE_STYLE,
}

_HE_COLS: list[str] = [f"HE{h}" for h in range(1, 25)]
_OUTPUT_COLS: list[str] = ["Date", "Type"] + _HE_COLS + ["OnPeak", "OffPeak", "Flat"]


def _hourly_dict_from_df(df: pd.DataFrame, value_col: str) -> dict[int, float]:
    """Map ``hour_ending -> value_col`` from a 24-row per-date forecast slice."""
    out: dict[int, float] = {}
    for _, r in df.iterrows():
        v = r.get(value_col)
        if pd.notna(v):
            out[int(r["hour_ending"])] = float(v)
    return out


def _row(
    target_date: date,
    type_label: str,
    hourly: dict[int, float],
) -> dict:
    """Build a Date | Type | HE1..HE24 | OnPeak | OffPeak | Flat row dict."""
    row: dict = {"Date": target_date, "Type": type_label}
    for h in range(1, 25):
        row[f"HE{h}"] = hourly.get(h)
    return add_summary_cols(row)


def _error_row(
    target_date: date,
    type_label: str,
    forecast_hourly: dict[int, float],
    actuals_hourly: dict[int, float],
) -> dict:
    err: dict[int, float] = {}
    for h in range(1, 25):
        a = actuals_hourly.get(h)
        f = forecast_hourly.get(h)
        if a is not None and f is not None:
            err[h] = f - a
    return _row(target_date, type_label, err)


def _onpeak_sort_key(row: dict) -> float:
    """Sort key — OnPeak mean, with NaN pushed to the end."""
    v = row.get("OnPeak")
    return float("inf") if v is None or pd.isna(v) else float(v)


def build_summary_table(
    target_date: date,
    df: pd.DataFrame,
    actuals_hourly: dict[int, float] | None,
) -> pd.DataFrame:
    """Build the four-series summary table (+ Actual / Error rows when settled).

    Row order: Actual? at top, then the four forecast series ranked by
    OnPeak (HE8-23) ascending, then Error rows in the same OnPeak-rank
    order (each series' Err row immediately following its forecast row
    would weave the table; instead Errors are grouped at the bottom in
    the same series order so the four forecasts read as one block).
    """
    rows: list[dict] = []
    if actuals_hourly is not None:
        rows.append(_row(target_date, "Actual", actuals_hourly))

    series_hourly: dict[str, dict[int, float]] = {}
    forecast_rows: list[dict] = []
    for label, col in SERIES_TO_COL.items():
        series_hourly[label] = _hourly_dict_from_df(df, col)
        forecast_rows.append(_row(target_date, label, series_hourly[label]))
    forecast_rows.sort(key=_onpeak_sort_key)
    rows.extend(forecast_rows)

    if actuals_hourly is not None:
        for fr in forecast_rows:
            label = fr["Type"]
            rows.append(
                _error_row(
                    target_date,
                    ERROR_TYPE_FOR_SERIES[label],
                    series_hourly[label],
                    actuals_hourly,
                )
            )

    return pd.DataFrame(rows, columns=_OUTPUT_COLS)


def _member_columns(df: pd.DataFrame) -> list[str]:
    """The 51 ECMWF member columns present in ``df``, sorted by index."""
    prefix = "da_price_ens_"
    members = [
        c for c in df.columns if c.startswith(prefix) and c[len(prefix) :].isdigit()
    ]
    return sorted(members, key=lambda c: int(c[len(prefix) :]))


def build_members_table(
    target_date: date,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Build the ENS-fan table.

    Rows: ENS Bottom (top of fan), then a mixed block of the 51 ECMWF
    members + Det + ENS Avg ranked by OnPeak (HE8-23) ascending, then
    ENS Top (bottom of fan). Det and ENS Avg are colored per
    ``_ROW_STYLES`` so they pop out among the dim member rows, showing
    where the point forecast and ensemble mean sit within the fan.
    Returns empty frame (with correct columns) when no member columns
    exist.
    """
    member_cols = _member_columns(df)
    rows: list[dict] = []

    rows.append(
        _row(
            target_date,
            "ENS Bottom",
            _hourly_dict_from_df(df, "da_price_ens_bottom"),
        )
    )

    middle_rows: list[dict] = []
    for col in member_cols:
        nn = col[len("da_price_ens_") :]
        middle_rows.append(
            _row(target_date, f"ENS_{nn}", _hourly_dict_from_df(df, col))
        )
    middle_rows.append(
        _row(target_date, "Det", _hourly_dict_from_df(df, "da_price_deterministic"))
    )
    middle_rows.append(
        _row(target_date, "ENS Avg", _hourly_dict_from_df(df, "da_price_ens_average"))
    )
    middle_rows.sort(key=_onpeak_sort_key)
    rows.extend(middle_rows)

    rows.append(
        _row(
            target_date,
            "ENS Top",
            _hourly_dict_from_df(df, "da_price_ens_top"),
        )
    )

    return pd.DataFrame(rows, columns=_OUTPUT_COLS)


def _gradient_color(val: float, max_abs: float) -> str:
    """ANSI 256-color escape for a signed value's |val|/max_abs bucket.

    Near-zero -> dark green (good — small error). Far from zero (in
    either direction) -> dark red (bad — large error). Buckets match the
    like-day printer so the two reports read visually identically.
    """
    if not _COLOR_ON or not np.isfinite(val) or max_abs <= 0:
        return ""
    norm = abs(val) / max_abs
    if norm < 0.10:
        return _DARK_GREEN_256
    if norm < 0.20:
        return _GREEN_256
    if norm < 0.35:
        return ""
    if norm < 0.65:
        return _RED_256
    return _DARK_RED_256


def _wrap_gradient(raw_cell: str, val: float, max_abs: float) -> str:
    color = _gradient_color(val, max_abs)
    if not color:
        return raw_cell
    stripped = raw_cell.lstrip(" ")
    leading = raw_cell[: len(raw_cell) - len(stripped)]
    return f"{leading}{color}{stripped}{_RS}"


def _print_table_header() -> str:
    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    print(header)
    print("-" * len(header))
    return header


def _format_row(row: pd.Series, signed: bool) -> str:
    """Build the string for one row's HE + summary cells (no row-style yet)."""
    line = f"{str(row['Date']):<12} {row['Type']:<10}"
    he_fmt = "+6.1f" if signed else "6.1f"
    blk_fmt = "+7.2f" if signed else "7.2f"
    for h in range(1, 25):
        v = row.get(f"HE{h}")
        line += f" {v:>{he_fmt}}" if pd.notna(v) else f" {'':>6}"
    for col in ("OnPeak", "OffPeak", "Flat"):
        v = row.get(col)
        line += f" {v:>{blk_fmt}}" if pd.notna(v) else f" {'':>7}"
    return line


def _format_row_with_gradient(row: pd.Series, max_abs: float) -> str:
    """Format an Error row with per-cell green->red gradient by |value|."""
    line = f"{str(row['Date']):<12} {row['Type']:<10}"
    for h in range(1, 25):
        v = row.get(f"HE{h}")
        if pd.notna(v):
            cell = f" {v:>+6.1f}"
            line += _wrap_gradient(cell, float(v), max_abs)
        else:
            line += f" {'':>6}"
    for col in ("OnPeak", "OffPeak", "Flat"):
        v = row.get(col)
        if pd.notna(v):
            cell = f" {v:>+7.2f}"
            line += _wrap_gradient(cell, float(v), max_abs)
        else:
            line += f" {'':>7}"
    return line


def print_config(
    target_date: date,
    hub: str,
    lead_days: int | None,
    det_exec: pd.Timestamp | None,
    ens_exec: pd.Timestamp | None,
) -> None:
    """Compact configuration block for the baseline run."""
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


def print_summary_table(table: pd.DataFrame) -> None:
    """Print the four-series summary table (Actual / 4 forecasts / Errors)."""
    print_section("Det + ENS Summary ($/MWh)")
    if table.empty:
        print("  (no rows)")
        return

    _print_table_header()

    error_types = set(ERROR_TYPE_FOR_SERIES.values())
    err_rows = table[table["Type"].isin(error_types)]
    err_max_abs = 0.0
    if len(err_rows):
        err_vals = err_rows[_HE_COLS].to_numpy(dtype=float).ravel()
        err_finite = err_vals[np.isfinite(err_vals)]
        if len(err_finite):
            err_max_abs = float(np.max(np.abs(err_finite)))

    for _, row in table.iterrows():
        is_err = row["Type"] in error_types
        if is_err:
            line = _format_row_with_gradient(row, err_max_abs)
        else:
            line = _format_row(row, signed=False)
            style = _ROW_STYLES.get(row["Type"])
            if style:
                line = f"{style}{line}{_RS}"
        print(line)

    print("-" * (len(_HE_COLS) * 7 + 12 + 11 + 7 * 3))


def print_members_table(
    table: pd.DataFrame,
    title: str = "ENS Members — ranked by OnPeak asc, between Bottom and Top ($/MWh)",
) -> None:
    """Print the ENS-fan table: bottom row, 51 ranked members, top row."""
    print_section(title)
    if table.empty:
        print("  (no rows)")
        return

    _print_table_header()

    for _, row in table.iterrows():
        line = _format_row(row, signed=False)
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        elif _COLOR_ON:
            # 51 individual members: dim so the eye lands on the floor / ceiling.
            line = f"{_DIM}{line}{_RS}"
        print(line)

    print("-" * (len(_HE_COLS) * 7 + 12 + 11 + 7 * 3))


# ── Dispersion footer (Width / IQR / Skew / Δ P50) ─────────────────────────
# Per-HE block-summary indices (matches the OnPk/OffPk/Flat convention used
# everywhere on the desk: HE8..HE23 OnPeak, HE1..HE7+HE24 OffPeak).
_BLOCK_HOUR_INDICES: tuple[tuple[str, np.ndarray], ...] = (
    ("OnPeak", np.array(range(7, 23), dtype=int)),
    ("OffPeak", np.array(list(range(0, 7)) + [23], dtype=int)),
    ("Flat", np.array(range(0, 24), dtype=int)),
)


def _per_he_array(df: pd.DataFrame, value_col: str) -> np.ndarray:
    """Return a length-24 array of ``value_col`` indexed by hour_ending,
    NaN where missing. Caller passes a ``df`` already restricted to the
    target date."""
    out = np.full(24, np.nan)
    for _, r in df.iterrows():
        h = int(r["hour_ending"])
        v = r.get(value_col)
        if 1 <= h <= 24 and pd.notna(v):
            out[h - 1] = float(v)
    return out


def _per_he_member_matrix(df: pd.DataFrame) -> np.ndarray:
    """Return a (24, n_members) matrix of the 51 ECMWF member prices,
    indexed by hour_ending. NaN where a member is missing for an HE."""
    member_cols = sorted(
        c
        for c in df.columns
        if c.startswith("da_price_ens_") and c[len("da_price_ens_") :].isdigit()
    )
    if not member_cols:
        return np.full((24, 0), np.nan)
    mat = np.full((24, len(member_cols)), np.nan)
    for _, r in df.iterrows():
        h = int(r["hour_ending"])
        if not (1 <= h <= 24):
            continue
        for j, c in enumerate(member_cols):
            v = r.get(c)
            if pd.notna(v):
                mat[h - 1, j] = float(v)
    return mat


@dataclass(frozen=True)
class DispersionMetrics:
    """Per-HE dispersion descriptors (length-24 arrays).

    All four are price-linear, so multiplying ``df`` by a uniform scale
    before computing them yields anchored values directly. ``Width`` and
    ``skew`` use Meteo's published Top / Bottom envelope; ``iqr`` and
    ``delta_p50`` are computed across the 51 ECMWF members.
    """

    width: np.ndarray  # ENS Top - ENS Bottom (Meteo published envelope)
    iqr: np.ndarray  # P75 - P25 across the 51 ECMWF members
    skew: np.ndarray  # (Top - P50) - (P50 - Bottom)
    delta_p50: np.ndarray  # first-difference of P50 across HEs (HE1 = NaN)


def compute_dispersion_metrics(df: pd.DataFrame) -> DispersionMetrics:
    """Build the per-HE dispersion descriptors for the target-date forecast
    frame ``df`` (one row per hour_ending; must carry ``da_price_ens_top``
    / ``da_price_ens_bottom`` and the 51 ``da_price_ens_NN`` member cols).
    """
    top = _per_he_array(df, "da_price_ens_top")
    bottom = _per_he_array(df, "da_price_ens_bottom")
    members = _per_he_member_matrix(df)

    width = top - bottom

    if members.shape[1] == 0:
        iqr = np.full(24, np.nan)
        p50 = np.full(24, np.nan)
    else:
        iqr = np.full(24, np.nan)
        p50 = np.full(24, np.nan)
        for h in range(24):
            row = members[h]
            row = row[np.isfinite(row)]
            if len(row) >= 2:
                iqr[h] = float(np.percentile(row, 75) - np.percentile(row, 25))
                p50[h] = float(np.percentile(row, 50))

    skew = (top - p50) - (p50 - bottom)

    delta_p50 = np.full(24, np.nan)
    delta_p50[1:] = np.diff(p50)

    return DispersionMetrics(width=width, iqr=iqr, skew=skew, delta_p50=delta_p50)


def _render_dispersion_row(
    date_str: str,
    label: str,
    values: np.ndarray,
    he_fmt: str,
    blk_fmt: str,
) -> None:
    """One Width / IQR / Skew / Δ P50 row with abs-low-is-good gradient
    coloring. Mirrors ``like_day_model_knn/pjm_rto_hourly/printers.py::
    _render_per_hour_metric_row``."""
    finite = values[np.isfinite(values)] if len(values) else np.array([])
    max_abs = float(max(abs(finite.min()), abs(finite.max()))) if len(finite) else 0.0

    line = f"{date_str:<12} {label:<10}"
    for h_idx in range(24):
        v = values[h_idx] if h_idx < len(values) else np.nan
        if np.isfinite(v):
            cell = f" {v:>{he_fmt}}"
            cell = _wrap_gradient(cell, float(v), max_abs)
            line += cell
        else:
            line += f" {'':>6}"
    for _, idx in _BLOCK_HOUR_INDICES:
        block_vals = [
            values[i] for i in idx if i < len(values) and np.isfinite(values[i])
        ]
        if block_vals:
            mean = float(np.mean(block_vals))
            cell = f" {mean:>{blk_fmt}}"
            cell = _wrap_gradient(cell, mean, max_abs)
            line += cell
        else:
            line += f" {'':>7}"
    print(line)


def print_dispersion_block(
    target_date: date,
    metrics: DispersionMetrics,
) -> None:
    """Footer block under a summary table: Width / IQR / Skew / Δ P50.

    No opening rule — relies on the preceding summary table's closing
    rule to separate. Closing rule emitted at the end so the block reads
    as a footer continuous with the summary above.
    """
    rule_width = len(_HE_COLS) * 7 + 12 + 11 + 7 * 3
    date_str = str(target_date)
    _render_dispersion_row(date_str, "Width", metrics.width, "6.2f", "7.2f")
    _render_dispersion_row(date_str, "IQR", metrics.iqr, "6.2f", "7.2f")
    _render_dispersion_row(date_str, "Skew", metrics.skew, "+6.2f", "+7.2f")
    _render_dispersion_row(date_str, "Δ P50", metrics.delta_p50, "+6.2f", "+7.2f")
    print("-" * rule_width)


# ── Three-section layout (mirrors like_day_model_knn print_quantiles +
# print_forecast + print_band_calibration) ─────────────────────────────────


def build_bands_table(
    target_date: date,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """ENS Bands table — four forecast rows sorted by OnPeak asc.

    No Actual / Error rows; pair this with ``build_forecast_vs_actuals``
    and ``build_bands_vs_actuals`` for the verification sections.
    """
    rows: list[dict] = []
    for label, col in SERIES_TO_COL.items():
        rows.append(_row(target_date, label, _hourly_dict_from_df(df, col)))
    rows.sort(key=_onpeak_sort_key)
    return pd.DataFrame(rows, columns=_OUTPUT_COLS)


def print_bands_section(
    target_date: date,
    bands_table: pd.DataFrame,
    dispersion: "DispersionMetrics | None" = None,
    title: str = "ENS Bands ($/MWh)",
) -> None:
    """Header + bands rows + (optional) dispersion footer."""
    print_section(title)
    print(
        f"  {_DIM}Per-HE bands: ENS Bottom = min envelope across 51 ECMWF "
        f"members at that hour; ENS Top = max envelope; ENS Avg = ensemble "
        f"mean. Det is the deterministic ECMWF point forecast (separate run).{_RS}"
    )
    print()
    if bands_table.empty:
        print("  (no rows)")
        return

    _print_table_header()
    for _, row in bands_table.iterrows():
        line = _format_row(row, signed=False)
        style = _ROW_STYLES.get(row["Type"])
        if style:
            line = f"{style}{line}{_RS}"
        print(line)
    print("-" * (len(_HE_COLS) * 7 + 12 + 11 + 7 * 3))

    if dispersion is not None:
        print_dispersion_block(target_date, dispersion)


def _block_metrics(
    forecast: np.ndarray,
    actual: np.ndarray,
    idx: np.ndarray,
) -> tuple[float | None, float | None, float | None]:
    """Return (mae, rmse, mape%) for a block of HE indices."""
    f = forecast[idx]
    a = actual[idx]
    mask = np.isfinite(f) & np.isfinite(a)
    if not mask.any():
        return None, None, None
    err = f[mask] - a[mask]
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err * err)))
    with np.errstate(divide="ignore", invalid="ignore"):
        denom = np.where(np.abs(a[mask]) < 1e-9, np.nan, np.abs(a[mask]))
        mape_arr = np.abs(err) / denom
    mape = float(np.nanmean(mape_arr) * 100.0) if np.isfinite(mape_arr).any() else None
    return mae, rmse, mape


def build_forecast_vs_actuals(
    target_date: date,
    df: pd.DataFrame,
    actuals_hourly: dict[int, float] | None,
    forecast_label: str = "Det",
) -> pd.DataFrame:
    """Actual / Forecast / Error / |Err| / MAPE % rows. Empty when no actuals.

    ``forecast_label`` selects which Meteo series serves as the central
    forecast — Det by default. Pre-scaled callers (e.g. the anchored
    pipeline) can pass an already-scaled ``df`` so values match the scaled
    bands above.
    """
    if actuals_hourly is None or df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLS)

    forecast_col = SERIES_TO_COL[forecast_label]
    forecast_hourly = _hourly_dict_from_df(df, forecast_col)

    actual_arr = np.array(
        [actuals_hourly.get(h, np.nan) for h in range(1, 25)], dtype=float
    )
    forecast_arr = np.array(
        [forecast_hourly.get(h, np.nan) for h in range(1, 25)], dtype=float
    )
    err_arr = forecast_arr - actual_arr
    abs_err_arr = np.abs(err_arr)
    with np.errstate(divide="ignore", invalid="ignore"):
        mape_pct = np.where(
            np.abs(actual_arr) > 1e-9,
            abs_err_arr / np.abs(actual_arr) * 100.0,
            np.nan,
        )

    def _arr_to_row(label: str, arr: np.ndarray) -> dict:
        d: dict[int, float] = {}
        for h_idx in range(24):
            v = arr[h_idx]
            if np.isfinite(v):
                d[h_idx + 1] = float(v)
        return _row(target_date, label, d)

    rows: list[dict] = []
    rows.append(_row(target_date, "Actual", actuals_hourly))
    rows.append(_arr_to_row("Forecast", forecast_arr))
    rows.append(_arr_to_row("Error", err_arr))
    rows.append(_arr_to_row("|Err|", abs_err_arr))
    rows.append(_arr_to_row("MAPE %", mape_pct))
    return pd.DataFrame(rows, columns=_OUTPUT_COLS)


def print_forecast_vs_actuals_section(
    target_date: date,
    table: pd.DataFrame,
) -> None:
    """Forecast-vs-Actuals header + table + RMSE footer line.

    Per-cell gradient on the Error / |Err| / MAPE % rows (abs-low-is-good).
    """
    print_section("Forecast vs Actuals")
    if table.empty:
        print(f"  {_DIM}(no settled DA LMP for the target date — skipping){_RS}")
        return

    _print_table_header()

    actual_row = table[table["Type"] == "Actual"]
    forecast_row = table[table["Type"] == "Forecast"]
    err_row = table[table["Type"] == "Error"]
    abs_err_row = table[table["Type"] == "|Err|"]
    mape_row = table[table["Type"] == "MAPE %"]

    err_max_abs = 0.0
    if len(err_row):
        err_vals = err_row[_HE_COLS].to_numpy(dtype=float).ravel()
        err_finite = err_vals[np.isfinite(err_vals)]
        if len(err_finite):
            err_max_abs = float(np.max(np.abs(err_finite)))

    abs_err_max = 0.0
    if len(abs_err_row):
        abs_vals = abs_err_row[_HE_COLS].to_numpy(dtype=float).ravel()
        abs_finite = abs_vals[np.isfinite(abs_vals)]
        if len(abs_finite):
            abs_err_max = float(np.max(abs_finite))

    mape_max = 0.0
    if len(mape_row):
        mape_vals = mape_row[_HE_COLS].to_numpy(dtype=float).ravel()
        mape_finite = mape_vals[np.isfinite(mape_vals)]
        if len(mape_finite):
            mape_max = float(np.max(mape_finite))

    for _, row in table.iterrows():
        t = row["Type"]
        if t == "Actual":
            line = _format_row(row, signed=False)
            style = _ROW_STYLES.get("Actual")
            if style:
                line = f"{style}{line}{_RS}"
        elif t == "Forecast":
            line = _format_row(row, signed=False)
            style = _ROW_STYLES.get("Det")  # central forecast styled like Det
            if style:
                line = f"{style}{line}{_RS}"
        elif t == "Error":
            line = _format_row_with_gradient(row, err_max_abs)
        elif t == "|Err|":
            # Same column shape as a non-signed value with abs-gradient coloring.
            cells = f"{str(row['Date']):<12} {row['Type']:<10}"
            for h in range(1, 25):
                v = row.get(f"HE{h}")
                if pd.notna(v):
                    raw = f" {v:>6.1f}"
                    cells += _wrap_gradient(raw, float(v), abs_err_max)
                else:
                    cells += f" {'':>6}"
            for col in ("OnPeak", "OffPeak", "Flat"):
                v = row.get(col)
                if pd.notna(v):
                    raw = f" {v:>7.2f}"
                    cells += _wrap_gradient(raw, float(v), abs_err_max)
                else:
                    cells += f" {'':>7}"
            line = cells
        elif t == "MAPE %":
            cells = f"{str(row['Date']):<12} {row['Type']:<10}"
            for h in range(1, 25):
                v = row.get(f"HE{h}")
                if pd.notna(v):
                    raw = f" {v:>5.1f}%"
                    cells += _wrap_gradient(raw, float(v), mape_max)
                else:
                    cells += f" {'':>6}"
            for col in ("OnPeak", "OffPeak", "Flat"):
                v = row.get(col)
                if pd.notna(v):
                    raw = f" {v:>6.1f}%"
                    cells += _wrap_gradient(raw, float(v), mape_max)
                else:
                    cells += f" {'':>7}"
            line = cells
        else:
            line = _format_row(row, signed=False)
        print(line)

    print("-" * (len(_HE_COLS) * 7 + 12 + 11 + 7 * 3))

    # RMSE footer line per block.
    if len(actual_row) and len(forecast_row):
        a = np.array(
            [actual_row.iloc[0].get(f"HE{h}", np.nan) for h in range(1, 25)],
            dtype=float,
        )
        f = np.array(
            [forecast_row.iloc[0].get(f"HE{h}", np.nan) for h in range(1, 25)],
            dtype=float,
        )
        parts: list[str] = []
        for name, idx in _BLOCK_HOUR_INDICES:
            _, rmse, _ = _block_metrics(f, a, idx)
            if rmse is not None:
                parts.append(f"{name}={rmse:.2f}")
        if parts:
            print(f"  RMSE:  {'   '.join(parts)}")


def _empirical_crps_per_he(
    df: pd.DataFrame,
    actuals_hourly: dict[int, float],
) -> np.ndarray:
    """Length-24 CRPS array, NaN where unavailable.

    Uses the energy-score form of CRPS for an empirical CDF from the 51
    ECMWF members at each HE:

        CRPS = (1/N) Σ |x_i - y| - (1/(2 N²)) Σ_i Σ_j |x_i - x_j|

    Returns NaN for hours with no actual or with fewer than 2 finite
    members.
    """
    crps = np.full(24, np.nan)
    members = _per_he_member_matrix(df)  # shape (24, n_members)
    if members.shape[1] == 0:
        return crps
    for h in range(24):
        actual = actuals_hourly.get(h + 1)
        if actual is None or pd.isna(actual):
            continue
        row = members[h]
        row = row[np.isfinite(row)]
        n = len(row)
        if n < 2:
            continue
        term1 = float(np.mean(np.abs(row - actual)))
        # Pairwise mean abs diff: (1/N²) Σ_i Σ_j |x_i - x_j|.
        pairwise = float(np.mean(np.abs(row[:, None] - row[None, :])))
        crps[h] = term1 - 0.5 * pairwise
    return crps


def build_bands_vs_actuals(
    target_date: date,
    df: pd.DataFrame,
    actuals_hourly: dict[int, float] | None,
) -> pd.DataFrame:
    """ENS Bottom / Actual / ENS Top / InBand / CRPS rows.

    InBand cells are ``"✓"`` when ``Bottom <= Actual <= Top``, ``"✗"``
    otherwise; OnPk/OffPk/Flat aggregates show a percent (rate of ✓).
    CRPS uses the 51-member empirical estimator.
    Empty when no actuals.
    """
    if actuals_hourly is None or df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLS)

    bottom = _hourly_dict_from_df(df, "da_price_ens_bottom")
    top = _hourly_dict_from_df(df, "da_price_ens_top")

    rows: list[dict] = []
    rows.append(_row(target_date, "ENS Bottom", bottom))
    rows.append(_row(target_date, "Actual", actuals_hourly))
    rows.append(_row(target_date, "ENS Top", top))

    # InBand row: ✓ / ✗ in HE cells, % in OnPk/OffPk/Flat aggregates.
    in_band: dict = {"Date": target_date, "Type": "InBand"}
    in_band_bool = np.full(24, np.nan)  # 1.0 / 0.0 / NaN per HE
    for h in range(1, 25):
        a = actuals_hourly.get(h)
        b = bottom.get(h)
        t = top.get(h)
        if a is None or b is None or t is None or pd.isna(a):
            in_band[f"HE{h}"] = None
            continue
        ok = (b - 1e-9) <= a <= (t + 1e-9)
        in_band[f"HE{h}"] = "✓" if ok else "✗"
        in_band_bool[h - 1] = 1.0 if ok else 0.0
    for name, idx in _BLOCK_HOUR_INDICES:
        block = in_band_bool[idx]
        block = block[np.isfinite(block)]
        in_band[name] = f"{int(round(np.mean(block) * 100))}%" if len(block) else None
    rows.append(in_band)

    # CRPS row: floats in HE cells + block means.
    crps_arr = _empirical_crps_per_he(df, actuals_hourly)
    crps: dict = {"Date": target_date, "Type": "CRPS"}
    for h_idx in range(24):
        v = crps_arr[h_idx]
        crps[f"HE{h_idx + 1}"] = float(v) if np.isfinite(v) else None
    for name, idx in _BLOCK_HOUR_INDICES:
        block = crps_arr[idx]
        block = block[np.isfinite(block)]
        crps[name] = float(np.mean(block)) if len(block) else None
    rows.append(crps)

    return pd.DataFrame(rows, columns=_OUTPUT_COLS)


def _format_inband_row(row: pd.Series) -> str:
    """InBand row: ✓/✗ in HE cells, % in block aggregates."""
    line = f"{str(row['Date']):<12} {row['Type']:<10}"
    for h in range(1, 25):
        v = row.get(f"HE{h}")
        cell = "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)
        # Per-HE cells are 6 wide (matches numeric format ` {v:>6.1f}`).
        line += f" {cell:>6}"
    for col in ("OnPeak", "OffPeak", "Flat"):
        v = row.get(col)
        cell = "" if v is None else str(v)
        line += f" {cell:>7}"
    return line


def _format_crps_row(row: pd.Series, max_abs: float) -> str:
    """CRPS row: low-is-good gradient (smaller CRPS = better)."""
    line = f"{str(row['Date']):<12} {row['Type']:<10}"
    for h in range(1, 25):
        v = row.get(f"HE{h}")
        if v is not None and not pd.isna(v):
            cell = f" {float(v):>6.3f}"
            cell = _wrap_gradient(cell, float(v), max_abs)
            line += cell
        else:
            line += f" {'':>6}"
    for col in ("OnPeak", "OffPeak", "Flat"):
        v = row.get(col)
        if v is not None and not pd.isna(v):
            cell = f" {float(v):>7.3f}"
            cell = _wrap_gradient(cell, float(v), max_abs)
            line += cell
        else:
            line += f" {'':>7}"
    return line


def print_bands_vs_actuals_section(
    target_date: date,
    table: pd.DataFrame,
) -> None:
    """Bands-vs-Actuals header + ENS Bottom / Actual / ENS Top / InBand / CRPS."""
    print_section("ENS Bands vs Actuals")
    if table.empty:
        print(f"  {_DIM}(no settled DA LMP for the target date — skipping){_RS}")
        return

    _print_table_header()

    crps_row = table[table["Type"] == "CRPS"]
    crps_max = 0.0
    if len(crps_row):
        crps_vals = []
        for h in range(1, 25):
            v = crps_row.iloc[0].get(f"HE{h}")
            if v is not None and not pd.isna(v):
                crps_vals.append(float(v))
        if crps_vals:
            crps_max = max(crps_vals)

    for _, row in table.iterrows():
        t = row["Type"]
        if t == "InBand":
            line = _format_inband_row(row)
        elif t == "CRPS":
            line = _format_crps_row(row, crps_max)
        else:
            line = _format_row(row, signed=False)
            style = _ROW_STYLES.get(t)
            if style:
                line = f"{style}{line}{_RS}"
        print(line)

    print("-" * (len(_HE_COLS) * 7 + 12 + 11 + 7 * 3))


__all__ = [
    "SERIES_TYPES",
    "SERIES_TO_COL",
    "ERROR_TYPE_FOR_SERIES",
    "DispersionMetrics",
    "build_summary_table",
    "build_members_table",
    "build_bands_table",
    "build_forecast_vs_actuals",
    "build_bands_vs_actuals",
    "compute_dispersion_metrics",
    "print_config",
    "print_summary_table",
    "print_members_table",
    "print_dispersion_block",
    "print_bands_section",
    "print_forecast_vs_actuals_section",
    "print_bands_vs_actuals_section",
]
