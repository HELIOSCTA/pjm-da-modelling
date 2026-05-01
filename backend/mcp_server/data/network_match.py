"""Match PJM outage tickets to PSS/E network branches.

Each active outage's ``facility_name`` is parsed for endpoint substations
and matched against the cached PSS/E parquets produced by
``backend.mcp_server.data.parse_psse_raw``.

Match strategies:

  - LINE:        match (sorted endpoint pair, voltage_kv) against
                 (from_name, to_name, voltage_kv) on PSS/E LINE branches.
                 Lines are undirected, so endpoints get sorted before lookup.
  - XFMR / PS:   match (station, voltage_kv) against either endpoint of
                 PSS/E XFMR branches. Stations with multiple transformers
                 (BEDINGTO has 4) flag ``ambiguous`` since we don't yet
                 parse the PJM TRAN/TX number for ckt_id disambiguation.

Substation-name normalization is first-token-uppercase: PJM facility
descriptions often append line IDs (e.g. ``"ELMONT4 553A"``) or voltage
tags (e.g. ``"BLACKOAK 500KV"``) that PSS/E names don't carry. Taking
the first token recovers the canonical substation in nearly every case.

Usage from a notebook or the MCP endpoint::

    from backend.mcp_server.data.network_match import (
        load_network, match_outages_to_branches, list_neighbors,
    )
    buses, branches = load_network()
    enriched = match_outages_to_branches(active_outages_df, branches, buses)
    neighbors = list_neighbors(from_bus, to_bus, branches, max_n=10)
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

from backend.mcp_server.data.parse_psse_raw import BRANCHES_PARQUET, BUSES_PARQUET

logger = logging.getLogger(__name__)


# ─── Loaders ─────────────────────────────────────────────────────────────────


def load_network(
    buses_path: Path = BUSES_PARQUET,
    branches_path: Path = BRANCHES_PARQUET,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load PSS/E buses + branches parquets produced by ``parse_psse_raw``.

    Raises FileNotFoundError with a hint to run the parser if the parquets
    aren't built yet.
    """
    if not buses_path.exists() or not branches_path.exists():
        raise FileNotFoundError(
            f"PSS/E parquets not found at {buses_path.parent}. "
            "Run: python -m backend.mcp_server.data.parse_psse_raw"
        )
    return pd.read_parquet(buses_path), pd.read_parquet(branches_path)


# ─── Facility-name parsing (mirrors views/transmission_outages._parse_facility) ─


def _parse_facility_endpoints(
    facility: str, equip_type: str,
) -> tuple[str | None, str | None, str | None, str | None]:
    """Return ``(from_station, to_station, single_station, leading_station)``.

    The PJM facility format is ``<EQUIP> <leading_station> <kV> KV <description>``.
    The leading station is more reliable than the description's first token
    (e.g. ``"XFMR FENTRES4 500 KV FENFRES4 TX5"`` has a typo in the description
    — leading is correct). For LINE the description carries the endpoints; for
    XFMR / PS the description is one substation but the leading station is a
    safer fallback if the description has typos.
    """
    if not facility:
        return None, None, None, None

    # Split out leading station (between equip-type prefix and voltage) and
    # description. Leading station can be multi-word (e.g. "BEAV DUQ", "21 KINCA").
    full = re.match(r"^\S+\s+(.+?)\s+\d+(?:\.\d+)?\s+KV\s+(.+)$", facility)
    if not full:
        return None, None, None, None
    leading = full.group(1).strip()
    desc = full.group(2).strip()

    if equip_type == "LINE":
        parts = re.split(r"\s*-\s*", desc, maxsplit=1)
        if len(parts) == 2:
            f = re.sub(r"\s+", " ", parts[0]).strip()
            t = re.sub(r"\s+", " ", parts[1]).strip()
            return f, t, None, leading
        return None, None, None, leading

    # XFMR, PS: first token of description is the substation
    station = desc.split()[0] if desc else None
    return None, None, station, leading


def _normalize_station(name: str | None) -> str | None:
    """First-token uppercase normalization."""
    if not name:
        return None
    s = name.strip()
    if not s:
        return None
    return s.split(maxsplit=1)[0].upper()


# Transformer label patterns. PJM and PSS/E both use varied conventions —
# ``TRAN 1``, ``TX5``, ``TR4``, ``XF1``, ``T2``, ``500-3T``, ``BK 7``,
# ``1 BANK``, ``230-1``, ``#1``. Patterns try most-specific first.
_XFMR_LABEL_PATTERNS = [
    re.compile(r"\bTRAN\s*0*(\d+)", re.IGNORECASE),         # TRAN 1, TRAN  3
    re.compile(r"\bTX\s*0*(\d+)", re.IGNORECASE),           # TX1, TX 5
    re.compile(r"\bXF\s*0*(\d+)", re.IGNORECASE),           # XF1, XF12
    re.compile(r"(?<![A-Z])TR\s*0*(\d+)(?![A-Z])"),         # TR4 (not TRAN)
    re.compile(r"(?<![A-Z\d])T\s*0*(\d+)(?![A-Z])"),        # bare T2, T11
    re.compile(r"\bBK\s*0*(\d+)", re.IGNORECASE),           # BK 7
    re.compile(r"(\d+)\s+BANK\b", re.IGNORECASE),           # 1 BANK
    re.compile(r"#\s*0*(\d+)"),                             # #1
    re.compile(r"\d+\s*-\s*0*(\d+)\b"),                     # 500-3, 230-1, 220-5
    re.compile(r"\b0*(\d+)\s*T\b", re.IGNORECASE),          # 8T, 4T (trailing T)
]


def _extract_xfmr_ckt_id(text: str | None) -> str | None:
    """Pull the transformer index out of a free-form label.

    Works on both PJM facility descriptions (``"BEDINGTO TRAN 1"``) and PSS/E
    name fields (``"BEDINGTO500 KV  TRAN  1"``). We avoid catching the voltage
    digits in PSS/E names like ``"500 KV"`` by anchoring patterns to specific
    label prefixes (TX/TR/TRAN/XF/T/BK) or to suffix tokens (Nth/N-N/#N).

    Returns the leading-zero-stripped digits as a string, or None.
    """
    if not text:
        return None
    # Strip the voltage marker so "500 KV" / "230 KV" don't pollute later digit
    # patterns. After "KV" comes the actual transformer label in PSS/E names.
    after_kv = re.split(r"\bKV\b", str(text), maxsplit=1)
    rest = after_kv[1] if len(after_kv) > 1 else str(text)
    for pat in _XFMR_LABEL_PATTERNS:
        m = pat.search(rest)
        if m:
            return str(int(m.group(1)))
    return None


# ─── Indexes ─────────────────────────────────────────────────────────────────


def _build_indexes(branches: pd.DataFrame) -> tuple[dict, dict]:
    """Build (line_index, xfmr_index) lookups for fast match.

    line_index keys: (frozenset({norm_a, norm_b}), voltage_int) -> list of branch row indices
    xfmr_index keys: (norm_station, voltage_int)               -> list of branch row indices
    """
    line_index: dict[tuple, list[int]] = {}
    xfmr_index: dict[tuple, list[int]] = {}

    for i, row in branches.iterrows():
        kv = int(round(float(row["voltage_kv"])))
        f_norm = _normalize_station(row["from_name"])
        t_norm = _normalize_station(row["to_name"])
        if row["equipment_type"] == "LINE" and f_norm and t_norm:
            key = (frozenset((f_norm, t_norm)), kv)
            line_index.setdefault(key, []).append(i)
        elif row["equipment_type"] == "XFMR":
            # Set-deduplicates: many transformers connect two buses at the same
            # substation (e.g. BEDINGTO 500↔138), so f_norm == t_norm — without
            # the set we'd index the same branch twice and break ckt-id
            # disambiguation later (one PSS/E branch would look like 2 hits).
            for endpoint in {f_norm, t_norm}:
                if endpoint:
                    xfmr_index.setdefault((endpoint, kv), []).append(i)
    return line_index, xfmr_index


# ─── Matcher ─────────────────────────────────────────────────────────────────


def match_outages_to_branches(
    outages_df: pd.DataFrame,
    branches_df: pd.DataFrame | None = None,
    buses_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Return ``outages_df`` enriched with PSS/E match metadata.

    Added columns:
      from_bus_psse, to_bus_psse  : matched PSS/E bus IDs (None if unmatched)
      rating_mva                  : MVA rating (RATEA) from PSS/E
      network_match_status        : 'matched' | 'ambiguous' | 'unmatched'
      neighbor_count              : 1-hop neighbor count on either endpoint
                                    (None for unmatched)

    A row is 'ambiguous' when the (endpoints, kV) lookup returns >1 PSS/E
    branch — typically a substation with multiple transformers and we don't
    yet parse the PJM TRAN/TX number to disambiguate.
    """
    if branches_df is None or buses_df is None:
        buses_df, branches_df = load_network()

    line_idx, xfmr_idx = _build_indexes(branches_df)

    df = outages_df.copy()
    matches: list[dict] = []

    for _, outage in df.iterrows():
        equip = outage.get("equipment_type", "")
        facility = outage.get("facility_name", "") or ""
        kv = int(round(float(outage["voltage_kv"]))) if pd.notna(outage["voltage_kv"]) else 0

        f_station, t_station, single_station, leading_station = _parse_facility_endpoints(facility, equip)
        f_norm = _normalize_station(f_station)
        t_norm = _normalize_station(t_station)
        s_norm = _normalize_station(single_station)
        l_norm = _normalize_station(leading_station)

        result = {
            "from_bus_psse": None,
            "to_bus_psse": None,
            "rating_mva": None,
            "network_match_status": "unmatched",
        }

        hits: list[int] = []
        if equip == "LINE" and f_norm and t_norm:
            hits = line_idx.get((frozenset((f_norm, t_norm)), kv), [])
            # Fallback: leading station may be cleaner than description's "from"
            if not hits and l_norm and t_norm and l_norm != f_norm:
                hits = line_idx.get((frozenset((l_norm, t_norm)), kv), [])
        elif equip in ("XFMR", "PS") and (s_norm or l_norm):
            hits = xfmr_idx.get((s_norm, kv), []) if s_norm else []
            # Fallback to leading-station match (handles description typos like
            # "XFMR FENTRES4 500 KV FENFRES4 TX5")
            if not hits and l_norm and l_norm != s_norm:
                hits = xfmr_idx.get((l_norm, kv), [])

        # XFMR / PS disambiguation: when multiple PSS/E transformers share the
        # same (station, kV), match the PJM TX/TRAN/XF/etc. number against the
        # same label parsed from the PSS/E ``name`` field. PSS/E ``ckt_id`` is
        # unrelated — at BEDINGTO, PJM "TRAN 1" maps to PSS/E ckt_id=4, but
        # PSS/E name="BEDINGTO500 KV  TRAN  1" lines up with PJM "TRAN 1".
        if hits and len(hits) > 1 and equip in ("XFMR", "PS"):
            target_label = _extract_xfmr_ckt_id(facility)
            if target_label:
                label_matches = [
                    h for h in hits
                    if _extract_xfmr_ckt_id(branches_df.loc[h, "name"]) == target_label
                ]
                if len(label_matches) == 1:
                    hits = label_matches  # promote to single-matched

        if hits:
            br = branches_df.loc[hits[0]]
            result.update(
                from_bus_psse=int(br["from_bus"]),
                to_bus_psse=int(br["to_bus"]),
                rating_mva=float(br["rating_mva"]) if pd.notna(br["rating_mva"]) else None,
                network_match_status="matched" if len(hits) == 1 else "ambiguous",
            )

        matches.append(result)

    enriched = pd.concat([df.reset_index(drop=True), pd.DataFrame(matches)], axis=1)

    # 1-hop neighbor count (only for rows we matched)
    enriched["neighbor_count"] = enriched.apply(
        lambda r: _count_neighbors(r["from_bus_psse"], r["to_bus_psse"], branches_df),
        axis=1,
    )

    return enriched


def _count_neighbors(
    from_bus: int | None, to_bus: int | None, branches: pd.DataFrame,
) -> int | None:
    """Number of PSS/E branches sharing either endpoint bus, excluding the
    matched branch itself (regardless of orientation)."""
    if from_bus is None or to_bus is None:
        return None
    endpoints = {from_bus, to_bus}
    touches_either = (
        branches["from_bus"].isin(endpoints) | branches["to_bus"].isin(endpoints)
    )
    is_self_orig = (branches["from_bus"] == from_bus) & (branches["to_bus"] == to_bus)
    is_self_rev = (branches["from_bus"] == to_bus) & (branches["to_bus"] == from_bus)
    return int((touches_either & ~is_self_orig & ~is_self_rev).sum())


def list_neighbors(
    from_bus: int,
    to_bus: int,
    branches_df: pd.DataFrame,
    max_n: int = 10,
) -> list[dict]:
    """Return up to ``max_n`` 1-hop neighbor branches, sorted by rating_mva desc.

    Excludes the matched branch itself (regardless of orientation).
    """
    endpoints = {from_bus, to_bus}
    touches_either = (
        branches_df["from_bus"].isin(endpoints) | branches_df["to_bus"].isin(endpoints)
    )
    is_self_orig = (branches_df["from_bus"] == from_bus) & (branches_df["to_bus"] == to_bus)
    is_self_rev = (branches_df["from_bus"] == to_bus) & (branches_df["to_bus"] == from_bus)

    sel = branches_df[touches_either & ~is_self_orig & ~is_self_rev]
    cols = [
        "from_bus", "to_bus", "from_name", "to_name",
        "voltage_kv", "equipment_type", "rating_mva", "ckt_id",
    ]
    return sel.nlargest(max_n, "rating_mva")[cols].to_dict(orient="records")
