"""Parse the PJM PSS/E v30 .raw network model into bus + branch parquets.

Reads ``backend/mcp_server/data/network/pjm_network_model.raw`` and writes
two parquet files under ``backend/cache/`` for downstream MCP enrichment:

  - ``psse_buses.parquet``    : bus_id, bus_name, voltage_kv, area, zone
  - ``psse_branches.parquet`` : LINE + 2-winding XFMR records with a unified
                                schema (from_bus, to_bus, from_name, to_name,
                                voltage_kv, equipment_type, rating_mva, ckt_id, name)

Why a custom parser: ``grg-pssedata 0.1.4`` only supports PSS/E v33; this PJM
file is v30 (different field counts per record). Pure-stdlib + pandas, ~200
lines.

Run as needed when the .raw file is refreshed (rare — quarterly when PJM
republishes the model). Idempotent — overwrites the parquets each run.

Usage::

    python -m backend.mcp_server.data.parse_psse_raw
"""
from __future__ import annotations

import csv
import logging
import sys
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parents[3]
RAW_FILE: Path = Path(__file__).parent / "network" / "pjm_network_model.raw"
CACHE_DIR: Path = _REPO_ROOT / "backend" / "cache"
BUSES_PARQUET: Path = CACHE_DIR / "psse_buses.parquet"
BRANCHES_PARQUET: Path = CACHE_DIR / "psse_branches.parquet"

# Section terminator marker — PSS/E uses "0 /End of <section> data, Begin <next>"
_END_MARKER = "0 /End of"


# ── Low-level record splitter ────────────────────────────────────────────────


def _split_record(line: str) -> list[str]:
    """Split one PSS/E record line into fields, honoring '...' quoted strings."""
    return next(csv.reader([line], quotechar="'", skipinitialspace=True))


def _section_bounds(lines: list[str]) -> dict[str, tuple[int, int]]:
    """Return (start_inclusive, end_exclusive) line indices per section.

    Sections are identified by the "End of <name> data" markers. The first
    section (Bus) starts after the 3-line header; subsequent sections start one
    line after the prior section's terminator.
    """
    bounds: dict[str, tuple[int, int]] = {}
    section_starts = {"Bus": 3}  # buses start after header (row 0) + 2 comment rows
    last_end: int | None = None

    for i, line in enumerate(lines):
        s = line.strip()
        if not s.startswith(_END_MARKER):
            continue
        # Format: "0 /End of Bus data, Begin Load data"
        head = s[len(_END_MARKER):].strip()
        end_name = head.split(" data")[0].strip()
        start = section_starts.get(end_name)
        if start is None and last_end is not None:
            start = last_end + 1
        if start is not None:
            bounds[end_name] = (start, i)

        # Capture next section's start
        if "Begin " in s:
            next_name = s.split("Begin ")[1].split(" data")[0].strip()
            section_starts[next_name] = i + 1
        last_end = i

    return bounds


# ── Buses ─────────────────────────────────────────────────────────────────────


def _parse_buses(lines: list[str], start: int, end: int) -> pd.DataFrame:
    """Bus records (v30, 11 fields): I, NAME, BASKV, IDE, GL, BL, AREA, ZONE, VM, VA, OWNER."""
    rows: list[dict] = []
    for i in range(start, end):
        try:
            fields = _split_record(lines[i])
            if len(fields) < 8:
                continue
            rows.append({
                "bus_id": int(fields[0].strip()),
                "bus_name": fields[1].strip(),
                "voltage_kv": float(fields[2]),
                "area": int(fields[6].strip()),
                "zone": int(fields[7].strip()),
            })
        except (ValueError, IndexError) as e:
            logger.warning(f"bus parse error at line {i}: {e!r} — skipping")
    return pd.DataFrame(rows)


# ── Branches (LINE) ───────────────────────────────────────────────────────────


def _parse_branches(lines: list[str], start: int, end: int) -> pd.DataFrame:
    """Branch records (v30): I, J, CKT, R, X, B, RATEA, RATEB, RATEC, ...

    field idx 6 = RATEA (long-term rating MVA, the one we want for capacity).
    A negative bus ID is a PSS/E metering flag — the actual bus is abs(id);
    we strip the sign so downstream joins to the bus table succeed.
    """
    rows: list[dict] = []
    for i in range(start, end):
        try:
            fields = _split_record(lines[i])
            if len(fields) < 9:
                continue
            rows.append({
                "from_bus": abs(int(fields[0].strip())),
                "to_bus": abs(int(fields[1].strip())),
                "third_bus": 0,
                "ckt_id": fields[2].strip(),
                "rating_mva": float(fields[6]),
                "equipment_type": "LINE",
                "name": "",
            })
        except (ValueError, IndexError) as e:
            logger.warning(f"branch parse error at line {i}: {e!r} — skipping")
    return pd.DataFrame(rows)


# ── Transformers (2-winding + 3-winding) ─────────────────────────────────────


def _parse_transformers(lines: list[str], start: int, end: int) -> pd.DataFrame:
    """Transformer records.

      Header (L1): I, J, K, CKT, CW, CZ, CM, MAG1, MAG2, NMETR, NAME, STAT, ...
        K = 0  → 2-winding (4 lines total)
        K != 0 → 3-winding (5 lines total)

      2-winding layout:
        L1 header  | L2 impedance (R1-2, X1-2, SBASE1-2)
        L3 winding 1 (WINDV1, NOMV1, ANG1, RATA1, ...)
        L4 winding 2 (WINDV2, NOMV2)

      3-winding layout:
        L1 header  | L2 impedances + VMSTAR/ANSTAR (11 fields)
        L3 winding 1   L4 winding 2   L5 winding 3 (16 fields each)

    For both we emit one row per device, indexed by the high-voltage winding's
    bus + voltage + rating. For 3-winding we use winding 1 (high side) as the
    primary view; the third (tertiary) winding is captured in ``third_bus``
    so neighbor traversal can still see it as connected to ``from_bus``.
    """
    rows: list[dict] = []
    parsed_2wind = 0
    parsed_3wind = 0
    i = start
    while i < end:
        try:
            header = _split_record(lines[i])
            if len(header) < 11:
                i += 1
                continue
            from_bus = abs(int(header[0].strip()))
            to_bus = abs(int(header[1].strip()))
            third = abs(int(header[2].strip()))
            ckt = header[3].strip()
            name = header[10].strip() if len(header) > 10 else ""

            if third == 0:
                # 2-winding: 4 lines total — winding 1 is at i + 2
                winding1 = _split_record(lines[i + 2])
                nomv1 = float(winding1[1])
                rata1 = float(winding1[3])
                rows.append({
                    "from_bus": from_bus,
                    "to_bus": to_bus,
                    "third_bus": 0,
                    "ckt_id": ckt,
                    "rating_mva": rata1,
                    "voltage_kv_native": nomv1,
                    "equipment_type": "XFMR",
                    "name": name,
                })
                parsed_2wind += 1
                i += 4
            else:
                # 3-winding: 5 lines total — winding 1 at i + 2
                winding1 = _split_record(lines[i + 2])
                nomv1 = float(winding1[1])
                rata1 = float(winding1[3])
                rows.append({
                    "from_bus": from_bus,
                    "to_bus": to_bus,
                    "third_bus": third,
                    "ckt_id": ckt,
                    "rating_mva": rata1,
                    "voltage_kv_native": nomv1,
                    "equipment_type": "XFMR",
                    "name": name,
                })
                parsed_3wind += 1
                i += 5
        except (ValueError, IndexError) as e:
            logger.warning(f"transformer parse error at line {i}: {e!r} — skipping 4 lines")
            i += 4

    logger.info(
        f"parsed {parsed_2wind:,} two-winding + {parsed_3wind:,} three-winding transformers"
    )
    return pd.DataFrame(rows)


# ── Top-level orchestration ──────────────────────────────────────────────────


def parse(raw_file: Path = RAW_FILE) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse the .raw and return (buses_df, branches_df).

    branches_df unifies LINE and XFMR records under a single schema with
    ``equipment_type`` discriminator. Each row carries from/to bus IDs +
    names + the from-bus voltage_kv (so a downstream join doesn't need both
    parquets). XFMR rows additionally have a ``name`` from the .raw header.
    """
    if not raw_file.exists():
        raise FileNotFoundError(f"PSS/E .raw not found: {raw_file}")

    logger.info(f"parsing {raw_file}")
    lines = raw_file.read_text(encoding="latin-1").splitlines()
    bounds = _section_bounds(lines)

    bus_start, bus_end = bounds["Bus"]
    br_start, br_end = bounds["Branch"]
    xf_start, xf_end = bounds["Transformer"]

    buses = _parse_buses(lines, bus_start, bus_end)
    lines_df = _parse_branches(lines, br_start, br_end)
    xfmrs_df = _parse_transformers(lines, xf_start, xf_end)

    bus_lookup = buses.set_index("bus_id")[["bus_name", "voltage_kv"]]

    # Join from-bus name / voltage onto each branch
    lines_df = lines_df.merge(
        bus_lookup.add_prefix("from_"), left_on="from_bus", right_index=True, how="left",
    ).merge(
        bus_lookup.add_prefix("to_"), left_on="to_bus", right_index=True, how="left",
    )
    lines_df["voltage_kv"] = lines_df["from_voltage_kv"]

    xfmrs_df = xfmrs_df.merge(
        bus_lookup.add_prefix("from_"), left_on="from_bus", right_index=True, how="left",
    ).merge(
        bus_lookup.add_prefix("to_"), left_on="to_bus", right_index=True, how="left",
    )
    # Prefer the xfmr's native NOMV1 (primary-side nominal) if populated; fall
    # back to the from-bus base kV.
    xfmrs_df["voltage_kv"] = xfmrs_df["voltage_kv_native"].where(
        xfmrs_df["voltage_kv_native"] > 0, xfmrs_df["from_voltage_kv"],
    )
    xfmrs_df = xfmrs_df.drop(columns=["voltage_kv_native"])

    cols = [
        "from_bus", "to_bus", "third_bus", "from_bus_name", "to_bus_name",
        "voltage_kv", "equipment_type", "rating_mva", "ckt_id", "name",
    ]
    branches = pd.concat(
        [lines_df[cols], xfmrs_df[cols]], ignore_index=True,
    )
    branches = branches.rename(columns={"from_bus_name": "from_name", "to_bus_name": "to_name"})

    return buses, branches


# ── Entry point ──────────────────────────────────────────────────────────────


def run(
    raw_file: Path = RAW_FILE,
    cache_dir: Path = CACHE_DIR,
) -> None:
    """Parse the .raw and write both parquets under ``cache_dir``."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    cache_dir.mkdir(parents=True, exist_ok=True)
    buses_path = cache_dir / BUSES_PARQUET.name
    branches_path = cache_dir / BRANCHES_PARQUET.name

    buses, branches = parse(raw_file)

    buses.to_parquet(buses_path, index=False)
    branches.to_parquet(branches_path, index=False)

    print("=== parse_psse_raw ===")
    print(f"source: {raw_file}")
    print()
    print(f"buses    rows={len(buses):>7,}  cols={list(buses.columns)}")
    print(f"  -> {buses_path}")
    print()
    print(f"branches rows={len(branches):>7,}  cols={list(branches.columns)}")
    print(f"  LINE: {(branches['equipment_type'] == 'LINE').sum():>7,}")
    print(f"  XFMR: {(branches['equipment_type'] == 'XFMR').sum():>7,}")
    print(f"  -> {branches_path}")
    print()
    print("voltage breakdown (branches):")
    counts = branches.groupby([branches["voltage_kv"].round().astype(int), "equipment_type"]).size().unstack(fill_value=0)
    print(counts.tail(10).to_string())


if __name__ == "__main__":
    run()
