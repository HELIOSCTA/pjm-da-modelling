"""View-model builders for the transmission-outage MCP endpoints.

One builder per dbt mart. Each returns a dict consumed by the markdown
formatter in `views/markdown_formatters.py`.

Mart → builder → endpoint mapping (see backend/mcp_server/main.py):

    pjm_transmission_outages_active               → build_active_view_model
    pjm_transmission_outages_window_7d            → build_window_7d_view_model
    pjm_transmission_outages_changes_24h_simple   → build_changes_24h_simple_view_model
    pjm_transmission_outages_changes_24h_snapshot → build_changes_24h_snapshot_view_model

Filter logic (Active/Approved, ≥230 kV, etc.) lives in dbt, not here.
"""
from __future__ import annotations

import re
from datetime import date

import numpy as np
import pandas as pd

# ─── Zone → region mapping (matches PJM pricing / congestion regions) ────────
_ZONE_MAP = {
    "AEP": "AEP / West",
    "COMED": "ComEd / West", "AMIL": "ComEd / West", "CWLP": "ComEd / West",
    "CONS": "MISO-seam / West", "WEC": "MISO-seam / West",
    "NIPS": "MISO-seam / West", "CIN": "MISO-seam / West",
    "DOM": "Dominion",
    "DAYTON": "Ohio Valley", "DEOK": "Ohio Valley",
    "EKPC": "Ohio Valley", "EKPCEL": "Ohio Valley",
    "FE": "FirstEnergy / Ohio", "CPP": "FirstEnergy / Ohio",
    "NXTERA": "FirstEnergy / Ohio",
    "DUQU": "West Penn / MidAtl", "PN": "West Penn / MidAtl",
    "PL-N": "West Penn / MidAtl", "PL-S": "West Penn / MidAtl",
    "PE": "West Penn / MidAtl",
    "PS-N": "East / NJ-DE-MD", "PS-S": "East / NJ-DE-MD",
    "JC-N": "East / NJ-DE-MD", "JC-S": "East / NJ-DE-MD",
    "RECO": "East / NJ-DE-MD", "AE": "East / NJ-DE-MD",
    "DPL": "East / NJ-DE-MD", "PEP": "East / NJ-DE-MD",
    "ME": "MidAtl / BGE-PEPCO", "SMECO": "MidAtl / BGE-PEPCO",
    "APSS": "MidAtl / BGE-PEPCO", "BC": "MidAtl / BGE-PEPCO",
    "NAEA": "MidAtl / BGE-PEPCO", "UGI": "MidAtl / BGE-PEPCO",
    "AMP": "MidAtl / BGE-PEPCO",
    "LGEE": "LGEE / South", "OVEC": "LGEE / South",
    "HTP": "NY ties", "NEPTUN": "NY ties", "LINVFT": "NY ties",
    "SENY": "NY ties", "UPNY": "NY ties",
}

# Prefix-based zone mapping (AEP-*, DOM-*, FE*)
_ZONE_PREFIX_MAP = {
    "AEP": "AEP / West",
    "DOM": "Dominion",
    "FE": "FirstEnergy / Ohio",
}

REGION_ORDER = [
    "AEP / West", "ComEd / West", "MISO-seam / West",
    "FirstEnergy / Ohio", "Ohio Valley",
    "Dominion", "West Penn / MidAtl",
    "MidAtl / BGE-PEPCO", "East / NJ-DE-MD",
    "LGEE / South", "NY ties",
]


def _map_zone_to_region(zone: str) -> str:
    """Map a PJM zone code to a congestion region."""
    if zone in _ZONE_MAP:
        return _ZONE_MAP[zone]
    for prefix, region in _ZONE_PREFIX_MAP.items():
        if zone.startswith(prefix):
            return region
    return zone  # unmapped zones keep their raw name


# Equipment type → congestion-impact category
_EQUIP_CATEGORY = {
    "LINE": "path",      # removes a flow corridor between two substations
    "XFMR": "capacity",  # removes local transformation capacity at a substation
    "PS": "capacity",    # removes phase-shifting capability at a substation
}


def _parse_facility(facility: str, equip_type: str) -> dict:
    """Parse facility name to extract route (lines) or station (equipment).

    Lines have two endpoints (from/to) identifying the flow path affected.
    Transformers and phase shifters reference a single substation.

    Returns dict with keys: from_station, to_station, station.
    """
    result = {"from_station": None, "to_station": None, "station": None}
    if not facility:
        return result

    match = re.search(r"\d+\s+KV\s+(.+)", facility)
    if not match:
        return result
    desc = match.group(1).strip()

    if equip_type == "LINE":
        parts = re.split(r"\s*-\s*", desc, maxsplit=1)
        if len(parts) == 2:
            result["from_station"] = re.sub(r"\s+", " ", parts[0]).strip()
            result["to_station"] = re.sub(r"\s+", " ", parts[1]).strip()
    else:
        result["station"] = desc.split()[0] if desc else None

    return result


# ─── Shared normalization ────────────────────────────────────────────────────


def _normalize(df: pd.DataFrame, reference_date: date) -> pd.DataFrame:
    """Type-coerce the mart frame and add derived columns shared by all builders.

    Adds:
      - region          : zone mapped via _map_zone_to_region
      - risk_flag       : risk == 'Yes'
      - equip_category  : path / capacity / other
      - days_out        : (reference_date - start_datetime).days  (negative if future)
      - days_to_return  : (end_datetime - reference_date).days    (NaN if past)
    """
    df = df.copy()
    df["zone"] = df["zone"].fillna("").astype(str)
    df["region"] = df["zone"].apply(_map_zone_to_region)
    df["voltage_kv"] = pd.to_numeric(df["voltage_kv"], errors="coerce").fillna(0).astype(int)
    df["start_datetime"] = pd.to_datetime(df["start_datetime"], errors="coerce")
    df["end_datetime"] = pd.to_datetime(df["end_datetime"], errors="coerce")
    df["last_revised"] = pd.to_datetime(df["last_revised"], errors="coerce")
    df["risk_flag"] = df["risk"].fillna("").astype(str).str.strip().str.lower() == "yes"
    df["equip_category"] = df["equipment_type"].map(_EQUIP_CATEGORY).fillna("other")

    ref_ts = pd.Timestamp(reference_date)
    df["days_out"] = (ref_ts - df["start_datetime"]).dt.days
    days_to_ret = (df["end_datetime"] - ref_ts).dt.days
    df["days_to_return"] = days_to_ret.where(df["end_datetime"] >= ref_ts)

    return df


def _outage_dict(row: pd.Series, *, include_diff: bool = False) -> dict:
    """Build one outage's record from a normalized row."""
    equip_type = row.get("equipment_type", "")
    parsed = _parse_facility(row.get("facility_name", ""), equip_type)
    cause_raw = row.get("cause", "") or ""
    cause_primary = cause_raw.split(";")[0].strip() if cause_raw else ""

    rec = {
        "ticket_id": _si(row.get("ticket_id")),
        "region": row.get("region"),
        "zone": row.get("zone"),
        "facility": row.get("facility_name", ""),
        "equip": equip_type,
        "equip_category": _EQUIP_CATEGORY.get(equip_type, "other"),
        "kv": int(row["voltage_kv"]) if pd.notna(row["voltage_kv"]) else None,
        "from_station": parsed["from_station"],
        "to_station": parsed["to_station"],
        "station": parsed["station"],
        "started": str(row["start_datetime"].date()) if pd.notna(row["start_datetime"]) else None,
        "est_return": str(row["end_datetime"].date()) if pd.notna(row["end_datetime"]) else None,
        "days_out": _si(row.get("days_out")),
        "days_to_return": _si(row.get("days_to_return")),
        "outage_state": row.get("outage_state"),
        "risk_flag": bool(row.get("risk_flag", False)),
        "cause": cause_primary,
    }
    if include_diff:
        rec["prev_outage_state"] = _clean_str(row.get("prev_outage_state"))
        rec["prev_status"] = _clean_str(row.get("prev_status"))
        prev_start = pd.to_datetime(row.get("prev_start_datetime"), errors="coerce")
        prev_end = pd.to_datetime(row.get("prev_end_datetime"), errors="coerce")
        rec["prev_start"] = str(prev_start.date()) if pd.notna(prev_start) else None
        rec["prev_end"] = str(prev_end.date()) if pd.notna(prev_end) else None
        rec["prev_risk"] = _clean_str(row.get("prev_risk"))
        prev_cause_raw = row.get("prev_cause") or ""
        rec["prev_cause"] = prev_cause_raw.split(";")[0].strip() if isinstance(prev_cause_raw, str) and prev_cause_raw else None
        rec["prev_equipment_count"] = _si(row.get("prev_equipment_count"))
        rec["diff_text"] = _build_diff_text(row)
    return rec


def _clean_str(val) -> str | None:
    """Return a clean string, or None for NaN / pd.NaT / empty."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip()
    return s if s else None


def _build_diff_text(row: pd.Series) -> str:
    """Compose 'end: 5/12 → 5/19, state: Approved → Active' from prev_* vs current.

    Compare start/end at date resolution (matching the display format), so a
    sub-day shift like 18:00 → 00:00 doesn't render as 'end: 5/12 → 5/12'.
    Use ``pd.notna`` for nullable risk/state strings — bare truthiness lets
    NaN floats through and produces 'risk: nan → nan' noise.
    """
    parts = []

    prev_state = row.get("prev_outage_state")
    cur_state = row.get("outage_state")
    if pd.notna(prev_state) and prev_state != cur_state:
        parts.append(f"state: {prev_state} → {cur_state}")

    prev_status = row.get("prev_status")
    cur_status = row.get("status")
    if pd.notna(prev_status) and prev_status != cur_status:
        parts.append(f"status: {prev_status} → {cur_status}")

    prev_start = pd.to_datetime(row.get("prev_start_datetime"), errors="coerce")
    cur_start = row.get("start_datetime")
    if pd.notna(prev_start) and pd.notna(cur_start) and prev_start != cur_start:
        parts.append(f"start: {_fmt_dt_diff(prev_start, cur_start)}")

    prev_end = pd.to_datetime(row.get("prev_end_datetime"), errors="coerce")
    cur_end = row.get("end_datetime")
    if pd.notna(prev_end) and pd.notna(cur_end) and prev_end != cur_end:
        parts.append(f"end: {_fmt_dt_diff(prev_end, cur_end)}")

    prev_risk = row.get("prev_risk")
    cur_risk = row.get("risk")
    if pd.notna(prev_risk) and prev_risk != cur_risk:
        parts.append(f"risk: {prev_risk} → {cur_risk}")

    # Compare cause at the primary-segment level (first segment before ';').
    # PJM bakes revision metadata into the trailing segments
    # (e.g., "Construction: New Equipment; Revised 04/09/2026 09:19; Cancelled
    # 02/02/2026 11:57"), which would otherwise emit a diff on every revision.
    prev_cause = _primary_cause(row.get("prev_cause"))
    cur_cause = _primary_cause(row.get("cause"))
    if prev_cause and cur_cause and prev_cause != cur_cause:
        parts.append(f"cause: {prev_cause} → {cur_cause}")

    prev_eq = row.get("prev_equipment_count")
    cur_eq = row.get("equipment_count")
    if pd.notna(prev_eq) and pd.notna(cur_eq) and int(prev_eq) != int(cur_eq):
        parts.append(f"equipment_count: {int(prev_eq)} → {int(cur_eq)}")

    return ", ".join(parts) if parts else "(no tracked field changed)"


def _primary_cause(val) -> str | None:
    """First segment of an eDART cause string, before any ';' metadata."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).split(";")[0].strip()
    return s if s else None


def _fmt_dt_diff(prev: pd.Timestamp, cur: pd.Timestamp) -> str:
    """Format a timestamp pair for diff display.

    If the dates differ → 'YYYY-MM-DD → YYYY-MM-DD' (most common case).
    If only time-of-day differs → 'YYYY-MM-DD HH:MM → YYYY-MM-DD HH:MM'
    so a sub-day push doesn't render as 'X → X'.
    """
    if prev.date() != cur.date():
        return f"{prev.date()} → {cur.date()}"
    return f"{prev.strftime('%Y-%m-%d %H:%M')} → {cur.strftime('%Y-%m-%d %H:%M')}"


def _si(val) -> int | None:
    """Safe int — return None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else int(f)
    except (TypeError, ValueError):
        return None


# ─── Region- and notable-summary helpers (shared) ────────────────────────────


def _build_regional_summary(df: pd.DataFrame) -> list[dict]:
    """One row per region: outage counts by voltage tier, risk count, chronic indicator."""
    rows: list[dict] = []
    seen: set[str] = set()

    for region in REGION_ORDER + sorted(set(df["region"]) - set(REGION_ORDER)):
        if region in seen:
            continue
        rdf = df[df["region"] == region]
        if rdf.empty:
            continue
        seen.add(region)

        rows.append({
            "region": region,
            "total": len(rdf),
            "path_count": int((rdf["equip_category"] == "path").sum()),
            "capacity_count": int((rdf["equip_category"] == "capacity").sum()),
            "count_765kv": int((rdf["voltage_kv"] == 765).sum()),
            "count_500kv": int((rdf["voltage_kv"] == 500).sum()),
            "count_345kv": int((rdf["voltage_kv"] == 345).sum()),
            "count_230kv": int((rdf["voltage_kv"] == 230).sum()),
            "risk_flagged": int(rdf["risk_flag"].sum()),
            "longest_out_days": _si(rdf["days_out"].max()),
            "soonest_return_days": _si(rdf.loc[rdf["days_to_return"].notna(), "days_to_return"].min()),
        })
    return rows


def _build_notable_outages(df: pd.DataFrame, reference_date: date) -> list[dict]:
    """Individual outages tagged with why they're notable."""
    notable: list[dict] = []
    seen_tickets: set = set()

    for _, row in df.iterrows():
        tags = []
        if row["risk_flag"]:
            tags.append("high-risk")
        if row["voltage_kv"] >= 500:
            tags.append("500kv+")
        if (pd.notna(row["start_datetime"])
                and (reference_date - row["start_datetime"].date()).days <= 3):
            tags.append("new")
        if (row["days_to_return"] is not None
                and not (isinstance(row["days_to_return"], float) and np.isnan(row["days_to_return"]))
                and row["days_to_return"] <= 7):
            tags.append("returning")

        if not tags:
            continue

        tid = row.get("ticket_id")
        if tid and tid in seen_tickets:
            continue
        if tid:
            seen_tickets.add(tid)

        rec = _outage_dict(row)
        rec["tags"] = tags
        notable.append(rec)

    def sort_key(r):
        risk_priority = 0 if "high-risk" in r["tags"] else 1
        return (risk_priority, -(r["kv"] or 0), r.get("days_to_return") or 9999)

    notable.sort(key=sort_key)
    return notable


# ─── Builder 1 — pjm_transmission_outages_active ─────────────────────────────


def build_active_view_model(
    df: pd.DataFrame, reference_date: date | None = None,
) -> dict:
    """View model for ``GET /views/transmission_outages_active``.

    Consumes the ``pjm_transmission_outages_active`` mart
    (Active/Approved, ≥230 kV, LINE/XFMR/PS).

    Sections produced:
      - regional_summary  : one row per region, voltage-tier counts + risk count
      - notable_outages   : tickets tagged high-risk / 500kv+ / new / returning
    """
    if df is None or df.empty:
        return {
            "reference_date": str(reference_date or date.today()),
            "error": "No active outage data available.",
        }

    if reference_date is None:
        reference_date = date.today()

    df = _normalize(df, reference_date)

    return {
        "reference_date": str(reference_date),
        "total_active": len(df),
        "regional_summary": _build_regional_summary(df),
        "notable_outages": _build_notable_outages(df, reference_date),
    }


# ─── Builder 2 — pjm_transmission_outages_window_7d ──────────────────────────


def build_window_7d_view_model(
    df: pd.DataFrame, reference_date: date | None = None,
) -> dict:
    """View model for ``GET /views/transmission_outages_window_7d``.

    Consumes the ``pjm_transmission_outages_window_7d`` mart
    (Active/Approved/Received, ≥230 kV, LINE/XFMR/PS, overlapping [now, now+7d]).
    Each row carries ``state_class`` ∈ {locked, planned}.

    Sections produced:
      - regional_summary : per-region counts split by state_class
      - locked_outages   : Active/Approved tickets in window, sorted by days_to_return
      - planned_outages  : Received tickets in window, sorted by start
    """
    if df is None or df.empty:
        return {
            "reference_date": str(reference_date or date.today()),
            "total": 0,
            "locked_count": 0,
            "planned_count": 0,
            "regional_summary": [],
            "locked_outages": [],
            "planned_outages": [],
        }

    if reference_date is None:
        reference_date = date.today()

    df = _normalize(df, reference_date)

    locked = df[df["state_class"] == "locked"]
    planned = df[df["state_class"] == "planned"]

    locked_records = sorted(
        [_outage_dict(r) for _, r in locked.iterrows()],
        key=lambda r: (
            r["days_to_return"] if r["days_to_return"] is not None else 9999,
            -(r["kv"] or 0),
        ),
    )
    planned_records = sorted(
        [_outage_dict(r) for _, r in planned.iterrows()],
        key=lambda r: (r["started"] or "9999", -(r["kv"] or 0)),
    )

    regional: list[dict] = []
    seen: set[str] = set()
    for region in REGION_ORDER + sorted(set(df["region"]) - set(REGION_ORDER)):
        if region in seen:
            continue
        rdf = df[df["region"] == region]
        if rdf.empty:
            continue
        seen.add(region)
        regional.append({
            "region": region,
            "total": len(rdf),
            "locked": int((rdf["state_class"] == "locked").sum()),
            "planned": int((rdf["state_class"] == "planned").sum()),
            "count_500kv_plus": int((rdf["voltage_kv"] >= 500).sum()),
            "risk_flagged": int(rdf["risk_flag"].sum()),
        })

    return {
        "reference_date": str(reference_date),
        "total": len(df),
        "locked_count": len(locked),
        "planned_count": len(planned),
        "regional_summary": regional,
        "locked_outages": locked_records,
        "planned_outages": planned_records,
    }


# ─── Builder 3 — pjm_transmission_outages_changes_24h_simple ─────────────────


def build_changes_24h_simple_view_model(
    df: pd.DataFrame, reference_date: date | None = None,
) -> dict:
    """View model for ``GET /views/transmission_outages_changes_24h_simple``.

    Consumes the ``pjm_transmission_outages_changes_24h_simple`` mart
    (last-24h delta driven by source ``created_at`` and ``last_revised``).

    Sections produced:
      - new_tickets     : tickets that first appeared in last 24h
      - revised_tickets : existing tickets PJM updated in last 24h

    Trade-off vs the snapshot variant: no diff text, no CLEARED detection.
    Useful from day 1 (no history baseline required).
    """
    if df is None or df.empty:
        return {
            "reference_date": str(reference_date or date.today()),
            "total_changes": 0,
            "new_count": 0,
            "revised_count": 0,
            "new_tickets": [],
            "revised_tickets": [],
        }

    if reference_date is None:
        reference_date = date.today()

    df = _normalize(df, reference_date)

    new_df = df[df["change_type"] == "NEW"].sort_values("voltage_kv", ascending=False)
    revised_df = df[df["change_type"] == "REVISED"].sort_values("voltage_kv", ascending=False)

    return {
        "reference_date": str(reference_date),
        "total_changes": len(df),
        "new_count": len(new_df),
        "revised_count": len(revised_df),
        "new_tickets": [_outage_dict(r) for _, r in new_df.iterrows()],
        "revised_tickets": [_outage_dict(r) for _, r in revised_df.iterrows()],
    }


# ─── Builder 4 — pjm_transmission_outages_changes_24h_snapshot ───────────────


def build_changes_24h_snapshot_view_model(
    df: pd.DataFrame, reference_date: date | None = None,
) -> dict:
    """View model for ``GET /views/transmission_outages_changes_24h_snapshot``.

    Consumes the ``pjm_transmission_outages_changes_24h_snapshot`` mart
    (last-24h delta driven by the SCD2 snapshot).

    Sections produced:
      - new_tickets     : tickets that first appeared in last 24h
      - revised_tickets : existing tickets PJM updated in last 24h, with diff_text
                          like "end: 5/12 → 5/19, state: Approved → Active"
      - cleared_tickets : tickets that vanished from PJM source in last 24h

    Returns all-zero counts (and a ``note`` field) for the first 24h after the
    snapshot is initialized — there's no history yet to diff against.
    """
    if df is None or df.empty:
        return {
            "reference_date": str(reference_date or date.today()),
            "total_changes": 0,
            "new_count": 0,
            "revised_count": 0,
            "cleared_count": 0,
            "new_tickets": [],
            "revised_tickets": [],
            "cleared_tickets": [],
            "note": (
                "Snapshot has not yet captured 24h of history. "
                "This view will populate after the second daily run."
            ),
        }

    if reference_date is None:
        reference_date = date.today()

    df = _normalize(df, reference_date)

    new_df = df[df["change_type"] == "NEW"].sort_values("voltage_kv", ascending=False)
    revised_df = df[df["change_type"] == "REVISED"].sort_values("voltage_kv", ascending=False)
    cleared_df = df[df["change_type"] == "CLEARED"].sort_values("voltage_kv", ascending=False)

    return {
        "reference_date": str(reference_date),
        "total_changes": len(df),
        "new_count": len(new_df),
        "revised_count": len(revised_df),
        "cleared_count": len(cleared_df),
        "new_tickets": [_outage_dict(r) for _, r in new_df.iterrows()],
        "revised_tickets": [_outage_dict(r, include_diff=True) for _, r in revised_df.iterrows()],
        "cleared_tickets": [_outage_dict(r) for _, r in cleared_df.iterrows()],
    }


# ─── Builder 5 — pjm_transmission_outages_active + PSS/E network enrichment ──


def build_network_view_model(
    enriched_df: pd.DataFrame,
    branches_df: pd.DataFrame,
    reference_date: date | None = None,
    max_neighbors: int = 5,
) -> dict:
    """View model for ``GET /views/transmission_outages_network``.

    Consumes the active mart enriched by
    ``backend.mcp_server.data.network_match.match_outages_to_branches`` —
    each row carries ``from_bus_psse``, ``to_bus_psse``, ``rating_mva``,
    ``network_match_status``, ``neighbor_count``.

    Sections produced:
      - match_coverage    : counts and percentages
      - matched_outages   : enriched outages, with up to ``max_neighbors``
                            1-hop neighbors per outage
      - ambiguous_outages : multi-match cases (typically multi-XFMR substations)
      - unmatched_outages : facilities not found in PSS/E (missing from model
                            or non-standard descriptions)
    """
    if enriched_df is None or enriched_df.empty:
        return {
            "reference_date": str(reference_date or date.today()),
            "error": "No active outage data.",
        }

    if reference_date is None:
        reference_date = date.today()

    df = _normalize(enriched_df, reference_date)

    # Late import to keep network_match optional for non-network endpoints
    from backend.mcp_server.data.network_match import list_neighbors

    matched = df[df["network_match_status"] == "matched"]
    ambiguous = df[df["network_match_status"] == "ambiguous"]
    unmatched = df[df["network_match_status"] == "unmatched"]

    n = len(df)
    coverage = {
        "total": n,
        "matched": int(len(matched)),
        "ambiguous": int(len(ambiguous)),
        "unmatched": int(len(unmatched)),
        "match_rate_pct": round(100 * (len(matched) + len(ambiguous)) / n, 1) if n else 0.0,
    }

    def _network_record(row: pd.Series, *, with_neighbors: bool) -> dict:
        rec = _outage_dict(row)
        from_b = row.get("from_bus_psse")
        to_b = row.get("to_bus_psse")
        rec["from_bus_psse"] = int(from_b) if pd.notna(from_b) else None
        rec["to_bus_psse"] = int(to_b) if pd.notna(to_b) else None
        rec["rating_mva"] = float(row["rating_mva"]) if pd.notna(row.get("rating_mva")) else None
        rec["neighbor_count"] = _si(row.get("neighbor_count"))
        rec["match_status"] = row.get("network_match_status")
        if with_neighbors and rec["from_bus_psse"] is not None and rec["to_bus_psse"] is not None:
            rec["neighbors"] = list_neighbors(
                rec["from_bus_psse"], rec["to_bus_psse"], branches_df, max_n=max_neighbors,
            )
        else:
            rec["neighbors"] = []
        return rec

    return {
        "reference_date": str(reference_date),
        "match_coverage": coverage,
        "matched_outages": [
            _network_record(r, with_neighbors=True)
            for _, r in matched.sort_values("voltage_kv", ascending=False).iterrows()
        ],
        "ambiguous_outages": [
            _network_record(r, with_neighbors=True)
            for _, r in ambiguous.sort_values("voltage_kv", ascending=False).iterrows()
        ],
        "unmatched_outages": [
            _network_record(r, with_neighbors=False)
            for _, r in unmatched.sort_values("voltage_kv", ascending=False).iterrows()
        ],
    }


# ─── Builder 6 — Tier 4: outages near constraint bus IDs ─────────────────────


def build_outages_for_constraints_view_model(
    enriched_df: pd.DataFrame,
    branches_df: pd.DataFrame,
    bus_ids: list[int],
    *,
    constraint_index: dict[int, list[str]] | None = None,
    reference_date: date | None = None,
    max_neighbors: int = 3,
) -> dict:
    """View model for ``GET /views/transmission_outages_for_constraints``.

    Filters active outages to those whose ``from_bus_psse`` or
    ``to_bus_psse`` is in ``bus_ids`` — the union of seed + 1-hop
    neighbor buses from the matched constraints in Tier 3.

    When ``constraint_index`` is provided (mapping bus_id → list of
    constraint names), each outage record gets a ``near_constraint_labels``
    field for cross-link annotation.
    """
    from backend.mcp_server.data.network_match import list_neighbors

    if reference_date is None:
        reference_date = date.today()

    bus_set = set()
    for b in bus_ids or []:
        try:
            bus_set.add(int(b))
        except (TypeError, ValueError):
            continue

    if enriched_df is None or enriched_df.empty or not bus_set:
        return {
            "reference_date": str(reference_date),
            "constraint_bus_count": len(bus_set),
            "total_active": 0 if enriched_df is None else len(enriched_df),
            "matched_count": 0,
            "outages": [],
        }

    df = _normalize(enriched_df, reference_date)
    fb = pd.to_numeric(df.get("from_bus_psse"), errors="coerce")
    tb = pd.to_numeric(df.get("to_bus_psse"), errors="coerce")
    mask = fb.isin(bus_set) | tb.isin(bus_set)
    hits = df[mask].copy()

    records: list[dict] = []
    for _, row in hits.sort_values("voltage_kv", ascending=False).iterrows():
        rec = _outage_dict(row)
        f_b = row.get("from_bus_psse")
        t_b = row.get("to_bus_psse")
        rec["from_bus_psse"] = int(f_b) if pd.notna(f_b) else None
        rec["to_bus_psse"] = int(t_b) if pd.notna(t_b) else None
        rec["rating_mva"] = float(row["rating_mva"]) if pd.notna(row.get("rating_mva")) else None
        rec["match_status"] = row.get("network_match_status")

        # Cross-link annotation
        near_buses: list[int] = []
        for v in (rec["from_bus_psse"], rec["to_bus_psse"]):
            if v is not None and v in bus_set:
                near_buses.append(v)
        rec["near_constraint_buses"] = near_buses

        labels: list[str] = []
        if constraint_index:
            for b in near_buses:
                for label in constraint_index.get(b, []):
                    if label not in labels:
                        labels.append(label)
        rec["near_constraint_labels"] = labels

        # Trim neighbors to a small set for compactness
        if rec["from_bus_psse"] is not None and rec["to_bus_psse"] is not None and max_neighbors > 0:
            rec["neighbors"] = list_neighbors(
                rec["from_bus_psse"], rec["to_bus_psse"],
                branches_df, max_n=max_neighbors,
            )
        else:
            rec["neighbors"] = []
        records.append(rec)

    return {
        "reference_date": str(reference_date),
        "constraint_bus_count": len(bus_set),
        "total_active": int(len(df)),
        "matched_count": int(len(hits)),
        "outages": records,
    }


# ─── Builder 7 — Pre-DA morning brief Tier 3: historical outages on bus IDs ──


def build_historical_outages_for_constraints_view_model(
    enriched_df: pd.DataFrame,
    branches_df: pd.DataFrame,
    bus_ids: list[int],
    *,
    binding_hours: list[int] | None = None,
    constraint_index: dict[int, list[str]] | None = None,
    window_start: date | None = None,
    window_end: date | None = None,
    reference_date: date | None = None,
    max_neighbors: int = 3,
) -> dict:
    """View model for ``GET /views/historical_outages_for_constraints``.

    Backward-looking variant of ``build_outages_for_constraints_view_model``.
    Filters outages whose [start_datetime, end_datetime] overlaps the window
    AND whose from_bus_psse / to_bus_psse intersects ``bus_ids``. Each
    record carries:

      - ``persistence_days``: days the outage was active within the window
      - ``persistence_class``: sustained (≥5d) / intermittent (2-4d) / transient (1d)
      - ``still_active_at_run``: bool — outage hasn't ended at the reference time
      - ``overlapping_he_count``: count of HEs in ``binding_hours`` covered by
        the outage window (rough proxy for "this outage was up during binding")
      - ``near_constraint_labels``: from ``constraint_index``
    """
    from backend.mcp_server.data.network_match import list_neighbors

    if reference_date is None:
        reference_date = date.today()
    if window_end is None:
        window_end = reference_date
    if window_start is None:
        window_start = window_end - pd.Timedelta(days=6).to_pytimedelta()

    bus_set: set[int] = set()
    for b in bus_ids or []:
        try:
            bus_set.add(int(b))
        except (TypeError, ValueError):
            continue

    if enriched_df is None or enriched_df.empty or not bus_set:
        return {
            "reference_date": str(reference_date),
            "window_start": str(window_start),
            "window_end": str(window_end),
            "binding_hours": list(binding_hours) if binding_hours else None,
            "constraint_bus_count": len(bus_set),
            "total_outages_in_window": 0 if enriched_df is None else len(enriched_df),
            "matched_count": 0,
            "outages": [],
        }

    df = _normalize(enriched_df, reference_date)
    fb = pd.to_numeric(df.get("from_bus_psse"), errors="coerce")
    tb = pd.to_numeric(df.get("to_bus_psse"), errors="coerce")
    mask = fb.isin(bus_set) | tb.isin(bus_set)
    hits = df[mask].copy()

    win_start_ts = pd.Timestamp(window_start)
    win_end_ts = pd.Timestamp(window_end) + pd.Timedelta(hours=23, minutes=59)
    binding_set = set(int(h) for h in (binding_hours or []))
    ref_ts = pd.Timestamp(reference_date)

    records: list[dict] = []
    for _, row in hits.sort_values(["voltage_kv"], ascending=False).iterrows():
        rec = _outage_dict(row)
        f_b = row.get("from_bus_psse")
        t_b = row.get("to_bus_psse")
        rec["from_bus_psse"] = int(f_b) if pd.notna(f_b) else None
        rec["to_bus_psse"] = int(t_b) if pd.notna(t_b) else None
        rec["rating_mva"] = float(row["rating_mva"]) if pd.notna(row.get("rating_mva")) else None
        rec["match_status"] = row.get("network_match_status")

        # Active interval clipped to the window
        start = row.get("start_datetime")
        end = row.get("end_datetime")
        eff_start = max(start, win_start_ts) if pd.notna(start) else win_start_ts
        eff_end = min(end, win_end_ts) if pd.notna(end) else win_end_ts
        active_days = max((eff_end - eff_start).days + 1, 0) if eff_end >= eff_start else 0
        rec["persistence_days"] = int(active_days)
        if active_days >= 5:
            rec["persistence_class"] = "sustained"
        elif active_days >= 2:
            rec["persistence_class"] = "intermittent"
        elif active_days >= 1:
            rec["persistence_class"] = "transient"
        else:
            rec["persistence_class"] = "none"

        rec["still_active_at_run"] = (
            pd.isna(end) or end >= ref_ts
        )

        # Binding-HE overlap: rough — does the outage cover the binding HE
        # range on at least one of its active days? Without per-hour activity
        # data this is "if active that day AND binding HEs are reasonable",
        # so we just flag yes/no here.
        if binding_set and active_days > 0:
            # If outage spans full days within the window, all binding HEs
            # are technically covered. Multi-hour partial-day coverage would
            # need start_datetime / end_datetime hour comparison; v1 uses the
            # full-day approximation.
            rec["overlapping_he_count"] = len(binding_set) * active_days
        else:
            rec["overlapping_he_count"] = 0

        # Cross-link annotation
        near_buses: list[int] = []
        for v in (rec["from_bus_psse"], rec["to_bus_psse"]):
            if v is not None and v in bus_set:
                near_buses.append(v)
        rec["near_constraint_buses"] = near_buses

        labels: list[str] = []
        if constraint_index:
            for b in near_buses:
                for label in constraint_index.get(b, []):
                    if label not in labels:
                        labels.append(label)
        rec["near_constraint_labels"] = labels

        if rec["from_bus_psse"] is not None and rec["to_bus_psse"] is not None and max_neighbors > 0:
            rec["neighbors"] = list_neighbors(
                rec["from_bus_psse"], rec["to_bus_psse"],
                branches_df, max_n=max_neighbors,
            )
        else:
            rec["neighbors"] = []
        records.append(rec)

    # Sort: persistence_days desc, then voltage_kv desc
    records.sort(key=lambda r: (-(r.get("persistence_days") or 0), -(r.get("kv") or 0)))

    return {
        "reference_date": str(reference_date),
        "window_start": str(window_start),
        "window_end": str(window_end),
        "binding_hours": list(binding_hours) if binding_hours else None,
        "constraint_bus_count": len(bus_set),
        "total_outages_in_window": int(len(df)),
        "matched_count": int(len(hits)),
        "outages": records,
    }
