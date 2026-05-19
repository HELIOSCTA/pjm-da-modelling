"""Price-duration-curve construction.

Per ``supply_stack_model.md`` Phase 5: sort all (target_date,
hour_ending) prices descending, separately for each model and for the
settled actuals. Overlay the two curves -- if the simulated curve is
consistently below actual in the top 5%, the model underprices
scarcity; if above in the bulk, VOM / heat rates are too high.

Returns a long DataFrame ``(model_name, rank_pct, price)`` ready for
``df.pivot`` or matplotlib. Rendering is deferred to the leaderboard
pipeline (which decides whether to write a PNG or just the underlying
numbers).
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_pdc_long(df: pd.DataFrame) -> pd.DataFrame:
    """One row per (model_name, rank_pct) with the sorted-descending price.

    Includes the settled actuals as a synthetic ``model_name='actual'``
    series so the leaderboard can overlay them directly. Drops NaN
    prices before ranking (an unforecasted hour or an unsettled date
    just shortens the series for that model)."""
    if df.empty:
        return pd.DataFrame(columns=["model_name", "rank_pct", "price"])

    pieces: list[pd.DataFrame] = []
    # One curve per model -- forecasted ``point``.
    for name, g in df.groupby("model_name", sort=False):
        s = g["point"].dropna().to_numpy(dtype=float)
        if len(s) == 0:
            continue
        s_sorted = np.sort(s)[::-1]
        rank_pct = (np.arange(1, len(s_sorted) + 1) / len(s_sorted)) * 100.0
        pieces.append(
            pd.DataFrame(
                {"model_name": str(name), "rank_pct": rank_pct, "price": s_sorted}
            )
        )
    # Plus the actuals as a single 'actual' series (dedup by (date, HE) so
    # the same settled LMP isn't counted N times across N models).
    actual = (
        df[["target_date", "hour_ending", "actual_lmp"]]
        .drop_duplicates(subset=["target_date", "hour_ending"])["actual_lmp"]
        .dropna()
        .to_numpy(dtype=float)
    )
    if len(actual) > 0:
        a_sorted = np.sort(actual)[::-1]
        rank_pct = (np.arange(1, len(a_sorted) + 1) / len(a_sorted)) * 100.0
        pieces.append(
            pd.DataFrame(
                {"model_name": "actual", "rank_pct": rank_pct, "price": a_sorted}
            )
        )
    if not pieces:
        return pd.DataFrame(columns=["model_name", "rank_pct", "price"])
    return pd.concat(pieces, ignore_index=True)
