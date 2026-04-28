"""Per-config target-vs-analogs fragment for forward_only_knn.

Each fragment block represents one named config from
``da_models.forward_only_knn.experiments.registry``. The block contains:

  * an HE1-24 line chart with the target day's Actual + Forecast plus the
    top-N analog days' actuals overlaid (faded by analog rank), and
  * a compact analog table (rank, date, distance, weight).

The analog LMPs come straight off ``run_forecast(...)["analogs"]`` — the
``lmp_h{1..24}`` columns are populated by ``similarity.engine.find_twins``
(``configs.LMP_LABEL_COLUMNS``), so no extra data pull is required.
"""
from __future__ import annotations

import contextlib
import io
import logging
from datetime import date
from typing import Any

import pandas as pd
import plotly.graph_objects as go

from da_models.forward_only_knn.configs import ForwardOnlyKNNConfig
from da_models.forward_only_knn.pipelines.forecast import run_forecast

logger = logging.getLogger(__name__)

Section = tuple[str, Any, str | None]

PLOTLY_TEMPLATE = "plotly_dark"
HOURS = list(range(1, 25))
LMP_COLS = [f"lmp_h{h}" for h in HOURS]
DEFAULT_TOP_N = 10

COLOR_ACTUAL = "#34d399"     # green
COLOR_FORECAST = "#f87171"   # red
COLOR_ANALOG = "#60a5fa"     # blue (faded per rank)


def build_fragments(
    target_date: date,
    config_name: str,
    config: ForwardOnlyKNNConfig,
    top_n: int = DEFAULT_TOP_N,
) -> list[Section]:
    """Run one config and return [(label, html_or_df, icon), ...] sections.

    Returns a single error fragment if the forecast run fails or yields no
    analogs — keeps the overall dashboard generation resilient to one bad
    config.
    """
    logger.info("Building forward_only_knn analog fragments for %s / %s", config_name, target_date)

    # run_forecast prints a lot to stdout; swallow it so the report build is quiet.
    with contextlib.redirect_stdout(io.StringIO()):
        try:
            result = run_forecast(target_date=target_date, config=config)
        except Exception as exc:
            logger.exception("run_forecast crashed for %s: %s", config_name, exc)
            return [(f"{config_name} — error", _empty(f"run_forecast crashed: {exc}"), None)]

    if "error" in result:
        return [(f"{config_name} — error", _empty(str(result["error"])), None)]

    analogs = result.get("analogs")
    output_table = result.get("output_table")
    if analogs is None or len(analogs) == 0 or output_table is None:
        return [(f"{config_name} — no analogs", _empty("No analogs returned."), None)]

    has_actuals = bool(result.get("has_actuals"))
    chart_html = _analog_chart_html(
        config_name=config_name,
        target_date=target_date,
        output_table=output_table,
        analogs=analogs,
        has_actuals=has_actuals,
        top_n=top_n,
        hub=config.hub,
    )

    table_df = _analog_table(analogs, top_n=top_n)
    actual_flat = _flat_value(output_table, "Actual") if has_actuals else None
    fcst_flat = _flat_value(output_table, "Forecast")
    err_flat = _flat_value(output_table, "Error") if has_actuals else None
    chart_label = _chart_label(config_name, fcst_flat, actual_flat, err_flat)

    return [
        (chart_label, chart_html, None),
        (f"{config_name} — analogs", table_df, None),
    ]


def _analog_chart_html(
    *,
    config_name: str,
    target_date: date,
    output_table: pd.DataFrame,
    analogs: pd.DataFrame,
    has_actuals: bool,
    top_n: int,
    hub: str,
) -> str:
    fig = go.Figure()

    top = analogs.head(top_n).copy()
    weights = pd.to_numeric(top.get("weight"), errors="coerce").fillna(0.0).to_numpy()
    w_max = float(weights.max()) if len(weights) and weights.max() > 0 else 1.0

    for _, row in top.iterrows():
        y = [row[c] if c in row and pd.notna(row[c]) else None for c in LMP_COLS]
        if not any(v is not None for v in y):
            continue
        rank = int(row.get("rank", 0))
        d = float(row.get("distance", float("nan")))
        w = float(row.get("weight", float("nan")))
        try:
            label = pd.to_datetime(row["date"]).strftime("%a %b-%d %Y")
        except Exception:
            label = str(row.get("date", "?"))
        opacity = 0.35 + 0.55 * (w / w_max if w_max else 0.0)
        fig.add_trace(go.Scatter(
            x=HOURS, y=y,
            mode="lines",
            name=f"#{rank} {label}  d={d:.3f} w={w:.3f}",
            line=dict(color=COLOR_ANALOG, width=1.2),
            opacity=opacity,
            hovertemplate=(
                f"<b>#{rank} {label}</b><br>"
                "HE %{x}: $%{y:,.2f}/MWh<extra></extra>"
            ),
        ))

    fcst_y = _row_hourly(output_table, "Forecast")
    if fcst_y is not None:
        fig.add_trace(go.Scatter(
            x=HOURS, y=fcst_y,
            mode="lines+markers",
            name=f"Forecast — {target_date}",
            line=dict(color=COLOR_FORECAST, width=2.5, dash="dash"),
            marker=dict(size=5),
            hovertemplate="<b>Forecast</b><br>HE %{x}: $%{y:,.2f}/MWh<extra></extra>",
        ))

    if has_actuals:
        act_y = _row_hourly(output_table, "Actual")
        if act_y is not None:
            fig.add_trace(go.Scatter(
                x=HOURS, y=act_y,
                mode="lines+markers",
                name=f"Actual — {target_date}",
                line=dict(color=COLOR_ACTUAL, width=2.5),
                marker=dict(size=5),
                hovertemplate="<b>Actual</b><br>HE %{x}: $%{y:,.2f}/MWh<extra></extra>",
            ))

    fig.update_layout(
        title=f"{config_name} — {hub} DA LMP — target vs top {len(top)} analogs",
        template=PLOTLY_TEMPLATE,
        height=460,
        margin=dict(l=60, r=40, t=60, b=60),
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02, font=dict(size=10)),
        hovermode="x unified",
    )
    fig.update_xaxes(
        title_text="Hour Ending",
        dtick=1, range=[0.5, 24.5], autorange=False, fixedrange=True,
    )
    fig.update_yaxes(title_text="$/MWh", gridcolor="rgba(99,110,250,0.1)")

    return fig.to_html(include_plotlyjs="cdn", full_html=False)


def _row_hourly(output_table: pd.DataFrame, row_type: str) -> list[float] | None:
    matches = output_table[output_table["Type"] == row_type]
    if matches.empty:
        return None
    row = matches.iloc[0]
    out = []
    for h in HOURS:
        v = row.get(f"HE{h}")
        out.append(None if pd.isna(v) else float(v))
    return out if any(v is not None for v in out) else None


def _flat_value(output_table: pd.DataFrame, row_type: str) -> float | None:
    matches = output_table[output_table["Type"] == row_type]
    if matches.empty:
        return None
    v = matches.iloc[0].get("Flat")
    return None if pd.isna(v) else float(v)


def _analog_table(analogs: pd.DataFrame, top_n: int) -> pd.DataFrame:
    cols = [c for c in ("rank", "date", "distance", "similarity", "weight") if c in analogs.columns]
    out = analogs.head(top_n)[cols].copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"]).dt.strftime("%a %Y-%m-%d")
    for c in ("distance", "similarity", "weight"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").map(lambda v: f"{v:.4f}" if pd.notna(v) else "")
    if "rank" in out.columns:
        out["rank"] = out["rank"].astype(int)
    return out.reset_index(drop=True)


def _chart_label(
    config_name: str,
    fcst_flat: float | None,
    actual_flat: float | None,
    err_flat: float | None,
) -> str:
    parts = [config_name]
    if fcst_flat is not None:
        parts.append(f"fcst={fcst_flat:.2f}")
    if actual_flat is not None:
        parts.append(f"act={actual_flat:.2f}")
    if err_flat is not None:
        parts.append(f"err={err_flat:+.2f}")
    return " | ".join(parts)


def _empty(msg: str) -> str:
    return f"<div style='padding:14px;color:#f87171;font-family:monospace;'>{msg}</div>"
