"""Print the Meteologica PJM DA price forecast loader as a wide table.

One section per series — deterministic point and the three ECMWF
ensemble summary statistics (average / bottom / top) — at Western Hub,
with OnPeak / OffPeak / Flat summaries and HE1..HE24, in $/MWh.

Mirrors ``check_loaders/pjm_lmp_total.py`` (single hub, $/MWh formatting)
and ``check_loaders/meteo_load.py`` (Forecast Executed + As of Date
metadata). The deterministic and ENS sides of the dbt mart are joined
FULL OUTER, so a row may carry det values without ENS or vice versa.

The Meteologica historical mart carries multiple vintages per
(forecast_date, hour_ending) keyed by ``as_of_date``. The loader
defaults to ``lead_days=1`` (DA-cutoff vintage,
``as_of_date == forecast_date - 1``).

Usage::

    python -m da_models.common.data.check_loaders.meteo_da_price
    python modelling/da_models/common/data/check_loaders/meteo_da_price.py
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402

from da_models.common.data import loader  # noqa: E402
from utils.logging_utils import init_logging, print_header, print_section  # noqa: E402

# ── Defaults (edit here instead of using CLI flags) ────────────────────────
SERIES: tuple[str, ...] = (
    "da_price_deterministic",
    "da_price_ens_average",
    "da_price_ens_bottom",
    "da_price_ens_top",
)
CACHE_DIR: Path | None = None
LOOKBACK_DAYS: int | None = 60  # set to None to print all dates
LOG_DIR: Path = _MODELLING_ROOT / "logs"

# Each series gets its forecast-execution metadata from the matching side
# of the FULL OUTER JOIN in the dbt mart: deterministic uses the det_*
# timestamp, the three ENS series share the ens_* timestamp.
_EXEC_DATETIME_COL: dict[str, str] = {
    "da_price_deterministic": "det_forecast_execution_datetime_local",
    "da_price_ens_average": "ens_forecast_execution_datetime_local",
    "da_price_ens_bottom": "ens_forecast_execution_datetime_local",
    "da_price_ens_top": "ens_forecast_execution_datetime_local",
}

HE_COLS: list[str] = [f"HE{h}" for h in range(1, 25)]
ONPEAK_HE_COLS: list[str] = [f"HE{h}" for h in range(8, 24)]
OFFPEAK_HE_COLS: list[str] = [c for c in HE_COLS if c not in ONPEAK_HE_COLS]
ORDERED_COLS: list[str] = [
    "Forecast Executed",
    "As of Date",
    "Date",
    "OnPeak",
    "OffPeak",
    "Flat",
    *HE_COLS,
]

_NUMERIC_COLS: list[str] = ["OnPeak", "OffPeak", "Flat", *HE_COLS]
_FORMATTERS: dict = {
    col: (lambda v: "" if pd.isna(v) else f"{v:>+8,.2f}") for col in _NUMERIC_COLS
}
_FORMATTERS["As of Date"] = lambda v: "" if pd.isna(v) else str(v)
_FORMATTERS["Forecast Executed"] = lambda v: (
    "" if pd.isna(v) else pd.Timestamp(v).strftime("%Y-%m-%d %H:%M")
)


def _meteo_da_price_wide_for_series(
    fcst: pd.DataFrame,
    series: str,
) -> pd.DataFrame:
    """Pivot the DA-price forecast frame to wide for a single series.

    Caller is responsible for any lookback windowing on ``fcst``. Rows
    where ``series`` is null (e.g. ENS missing on the det side of the
    FULL OUTER JOIN) are dropped before pivoting.
    """
    if series not in fcst.columns:
        return pd.DataFrame(columns=ORDERED_COLS)

    df = fcst[fcst[series].notna()]
    if df.empty:
        return pd.DataFrame(columns=ORDERED_COLS)

    pivot = df.pivot_table(
        index="date",
        columns="hour_ending",
        values=series,
        aggfunc="mean",
    ).reindex(columns=range(1, 25))
    pivot.columns = [f"HE{h}" for h in pivot.columns]
    pivot["OnPeak"] = pivot[ONPEAK_HE_COLS].mean(axis=1)
    pivot["OffPeak"] = pivot[OFFPEAK_HE_COLS].mean(axis=1)
    pivot["Flat"] = pivot[HE_COLS].mean(axis=1)
    pivot = pivot.reset_index()

    exec_col = _EXEC_DATETIME_COL.get(series)
    meta_cols = ["date"]
    if "as_of_date" in df.columns:
        meta_cols.append("as_of_date")
    if exec_col is not None and exec_col in df.columns:
        meta_cols.append(exec_col)
    meta = df[meta_cols].drop_duplicates(subset=["date"], keep="first")
    pivot = pivot.merge(meta, on="date", how="left")

    rename_map = {"date": "Date", "as_of_date": "As of Date"}
    if exec_col is not None:
        rename_map[exec_col] = "Forecast Executed"
    pivot = pivot.rename(columns=rename_map)

    if "As of Date" not in pivot.columns:
        pivot["As of Date"] = pd.NaT
    if "Forecast Executed" not in pivot.columns:
        pivot["Forecast Executed"] = pd.NaT

    return (
        pivot[ORDERED_COLS].sort_values("Date", ascending=False).reset_index(drop=True)
    )


def build_meteo_da_price_table(
    series: str = SERIES[0],
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Return the wide Meteologica DA-price table for ``series``, sorted Date desc.

    ``series`` is one of ``da_price_deterministic``, ``da_price_ens_average``,
    ``da_price_ens_bottom``, ``da_price_ens_top``. ``lookback_days`` trims to
    the N most recent dates (inclusive of the latest date in the data);
    ``None`` returns every date.

    Columns: Forecast Executed | As of Date | Date | OnPeak | OffPeak | Flat
    | HE1..HE24 (in $/MWh).
    """
    fcst = loader.load_meteologica_da_price_forecast(cache_dir=cache_dir)
    if lookback_days is not None and not fcst.empty:
        cutoff = fcst["date"].max() - timedelta(days=lookback_days - 1)
        fcst = fcst[fcst["date"] >= cutoff]
    return _meteo_da_price_wide_for_series(fcst, series)


def _print_meteo_da_price_series_block(
    pl,
    fcst: pd.DataFrame,
    series: str,
) -> None:
    """Print one series' DA-price section: header, metadata, table."""
    print_section(f"WESTERN HUB ({series})")

    table = _meteo_da_price_wide_for_series(fcst, series)
    if table.empty:
        pl.warning(f"No DA-price data for series={series}.")
        return

    date_min = table["Date"].min()
    date_max = table["Date"].max()
    flat_min = table["Flat"].min()
    flat_max = table["Flat"].max()
    pl.info(f"{series}: rows={len(table):,} | date range: {date_min} -> {date_max}")
    pl.info(f"{series}: Flat range: ${flat_min:+,.2f} -> ${flat_max:+,.2f} /MWh")

    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.width",
        None,
    ):
        print(table.to_string(index=False, formatters=_FORMATTERS))


def _print_forward_horizon(
    pl,
    cache_dir: Path | None,
    series_list: tuple[str, ...],
) -> None:
    """Print the forward multi-day Meteologica DA-price horizon.

    Uses ``latest_only=True`` to surface every forecast_date in the
    most-recent vintage. The DA-price forecast is single-node (Western
    Hub) so there's no region dimension; ``latest_only`` picks one global
    most-recent ``as_of_date``.
    """
    print_header("Forward horizon (latest publish, WESTERN HUB)")

    with pl.timer("load Meteologica DA-price forecast (latest_only=True)"):
        latest = loader.load_meteologica_da_price_forecast(
            cache_dir=cache_dir, latest_only=True
        )

    if latest.empty:
        pl.warning("latest_only frame is empty; no forward horizon to print.")
        return

    pl.info(
        f"As of {latest['as_of_date'].max()}: "
        f"{latest['date'].nunique()} forecast_date(s) "
        f"({latest['date'].min()} -> {latest['date'].max()})"
    )

    for series in series_list:
        print_section(f"WESTERN HUB ({series}) — forward horizon")
        table = _meteo_da_price_wide_for_series(latest, series)
        if table.empty:
            pl.warning(f"No forward-horizon rows for series={series}.")
            continue
        table = table.sort_values("Date", ascending=True).reset_index(drop=True)
        with pd.option_context(
            "display.max_rows",
            None,
            "display.max_columns",
            None,
            "display.width",
            None,
        ):
            print(table.to_string(index=False, formatters=_FORMATTERS))


def run(
    series_list: tuple[str, ...] = SERIES,
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="check_loaders_meteo_da_price", log_dir=LOG_DIR)
    try:
        _print_forward_horizon(pl, cache_dir, series_list)

        lookback_label = (
            f"last {lookback_days}d" if lookback_days is not None else "all dates"
        )
        print_header(f"Historical realization -- WESTERN HUB ({lookback_label})")

        with pl.timer("load Meteologica DA-price forecast (lead_days=1)"):
            fcst = loader.load_meteologica_da_price_forecast(cache_dir=cache_dir)

        if fcst.empty:
            pl.warning("Meteologica DA-price frame is empty; nothing to print.")
            return

        if lookback_days is not None:
            cutoff = fcst["date"].max() - timedelta(days=lookback_days - 1)
            fcst = fcst[fcst["date"] >= cutoff]

        for series in series_list:
            _print_meteo_da_price_series_block(pl, fcst, series)

        pl.success(f"Printed {len(series_list)} series: {', '.join(series_list)}.")
    finally:
        pl.close()


if __name__ == "__main__":
    run()
