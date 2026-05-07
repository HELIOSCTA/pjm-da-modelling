"""ICE blotter PnL — join cleared deals to peak DA/RT settles at PJM WH.

Parses the multi-section ICE export (`ice_trade_blotter.csv`), pulls just
the Cleared Deals table, joins each row to the peak-block (HE 8-23)
average LMP at PJM Western Hub on its Begin Date — DA settle for
``PJM WH DA (Daily)`` rows, RT settle for ``PJM WH RT`` rows — and
writes per-trade and cumulative PnL to `ice_pnl.csv` next to the source.

Sign convention: Sold +1, Bought -1 → ``pnl = sign * (price - settle) * mwh``.

Usage::

    python fundies/research/@ICE-Trade-Blotter/blotter_pnl.py
"""
from __future__ import annotations

import sys
from io import StringIO
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[3] / "modelling"
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402

from da_models.common.data import loader  # noqa: E402

# ── Defaults (edit here instead of using CLI flags) ────────────────
BLOTTER_CSV: Path = Path(__file__).resolve().parent / "ice_trade_blotter.csv"
OUTPUT_CSV: Path = Path(__file__).resolve().parent / "ice_pnl.csv"
HUB: str = "WESTERN HUB"
PEAK_HOURS: tuple[int, ...] = tuple(range(8, 24))  # HE 0800..HE 2300

_DA_KEY = "DA"
_RT_KEY = "RT"


def _parse_cleared_section(csv_path: Path) -> pd.DataFrame:
    """Extract only the Cleared Deals table from the ICE export."""
    raw = csv_path.read_text(encoding="utf-8-sig").splitlines()

    start = next(
        (i for i, line in enumerate(raw)
         if line.strip().lower().startswith("cleared deals")),
        None,
    )
    if start is None:
        raise ValueError("Could not find 'Cleared Deals' section in blotter")

    header_idx = next(
        (i for i in range(start + 1, len(raw)) if raw[i].strip(",").strip()),
        None,
    )
    if header_idx is None:
        raise ValueError("No header row found under Cleared Deals")

    end_idx = len(raw)
    for i in range(header_idx + 1, len(raw)):
        if not raw[i].strip(",").strip():
            end_idx = i
            break

    block = "\n".join(raw[header_idx:end_idx])
    return pd.read_csv(StringIO(block))


def _market_key(product: str) -> str | None:
    p = (product or "").upper()
    if "DA" in p:
        return _DA_KEY
    if "RT" in p:
        return _RT_KEY
    return None


def peak_settle_table(df_lmp: pd.DataFrame, hub: str) -> pd.DataFrame:
    """One row per date, peak-block (HE 8-23) average LMP at the hub."""
    df = df_lmp[df_lmp["region"] == hub].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")
    df["lmp"] = pd.to_numeric(df["lmp"], errors="coerce")
    df = df[df["hour_ending"].isin(PEAK_HOURS)].dropna(subset=["lmp"])
    return (
        df.groupby("date", as_index=False)["lmp"]
        .mean()
        .rename(columns={"lmp": "settle"})
    )


def compute_pnl(
    blotter_csv: Path = BLOTTER_CSV,
    hub: str = HUB,
) -> pd.DataFrame:
    deals = _parse_cleared_section(blotter_csv)
    deals = deals.dropna(
        subset=["Trade Date", "B/S", "Product", "Begin Date", "Price"]
    ).copy()

    deals["flow_date"] = pd.to_datetime(
        deals["Begin Date"], format="%d-%b-%y", errors="coerce"
    ).dt.date
    deals["trade_date"] = pd.to_datetime(
        deals["Trade Date"], format="%d-%b-%y", errors="coerce"
    ).dt.date
    deals["price"] = pd.to_numeric(deals["Price"], errors="coerce")
    deals["mwh"] = pd.to_numeric(deals["Total Quantity"], errors="coerce")
    deals["sign"] = deals["B/S"].str.lower().map({"sold": 1, "bought": -1})
    deals["market"] = deals["Hub"].map(_market_key)

    da = peak_settle_table(loader.load_lmps_da(), hub).assign(market=_DA_KEY)
    rt = peak_settle_table(loader.load_lmps_rt(), hub).assign(market=_RT_KEY)
    settles = pd.concat([da, rt], ignore_index=True)
    settles["market"] = settles["market"].astype(object)
    deals["market"] = deals["market"].astype(object)

    merged = deals.merge(
        settles,
        left_on=["flow_date", "market"],
        right_on=["date", "market"],
        how="left",
    )
    merged["pnl"] = merged["sign"] * (merged["price"] - merged["settle"]) * merged["mwh"]
    merged = merged.sort_values(["flow_date", "trade_date"]).reset_index(drop=True)
    merged["cum_pnl"] = merged["pnl"].cumsum()

    out_cols = [
        "trade_date", "flow_date", "Product", "Hub", "B/S",
        "price", "settle", "mwh", "pnl", "cum_pnl",
    ]
    return merged[out_cols].rename(
        columns={"Product": "product", "Hub": "hub", "B/S": "side"}
    )


def run(
    blotter_csv: Path = BLOTTER_CSV,
    output_csv: Path = OUTPUT_CSV,
    hub: str = HUB,
    quiet: bool = False,
) -> pd.DataFrame:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pnl = compute_pnl(blotter_csv=blotter_csv, hub=hub)
    pnl.to_csv(output_csv, index=False)

    if quiet:
        return pnl

    print(f"=== ICE Blotter PnL | hub={hub} | peak HE {PEAK_HOURS[0]}-{PEAK_HOURS[-1]} ===")
    fmt = {
        "price":   lambda v: f"{v:>7,.2f}" if pd.notna(v) else "      -",
        "settle":  lambda v: f"{v:>7,.2f}" if pd.notna(v) else "      -",
        "mwh":     lambda v: f"{v:>5,.0f}" if pd.notna(v) else "    -",
        "pnl":     lambda v: f"{v:>9,.0f}" if pd.notna(v) else "        -",
        "cum_pnl": lambda v: f"{v:>9,.0f}" if pd.notna(v) else "        -",
    }
    print(pnl.to_string(index=False, formatters=fmt))

    realized = pnl["pnl"].dropna()
    unsettled = pnl["pnl"].isna().sum()
    print()
    print("--- Summary ---")
    print(f"trades scored  : {len(realized)} / {len(pnl)} ({unsettled} unsettled)")
    print(f"realized P&L   : ${realized.sum():>11,.0f}")
    print(f"  wins         : {(realized > 0).sum():>3}  ${realized[realized > 0].sum():>11,.0f}")
    print(f"  losses       : {(realized < 0).sum():>3}  ${realized[realized < 0].sum():>11,.0f}")
    print(f"output written : {output_csv}")
    return pnl


if __name__ == "__main__":
    run()
