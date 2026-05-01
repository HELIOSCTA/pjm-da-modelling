"""Match PJM binding constraints to PSS/E network branches.

Constraint ``monitored_facility`` strings come in three distinct conventions
and one residual category:

  - DA underscore-coded   : ``CONASTON_500KVCNS-PEA_1_LN``
                            ``BURMA_T1_138T1_XF``
  - RT EMS fixed-width    : ``CONASTON-PEACHBOT 5012      B  500 KV``
                            ``CLOUD TX1 XFORMER           L  115 KV``
  - Prose l/o (in either) : ``Batesville-Hubble 138 l/o Tanners Crk-Miami Fort 345``
  - INTERFACE / unknown   : ``APSOUTH``, ``BCPEP``, ``APSOUTH contingency 22``

The DA shape is the trickiest — for LINE constraints, only the first endpoint
is given as a full name; the second endpoint is a 3-letter abbreviation
(e.g. ``CNS-PEA`` = CONASTONE → PEACHBOTTOM). We resolve that by matching the
leading station + voltage against PSS/E LINE branches, then disambiguating
by prefix-checking the second endpoint's abbreviation against the other end's
PSS/E station name.

PSS/E indexes are reused from ``network_match.py`` (line + xfmr indexes).

Usage::

    from backend.mcp_server.data.network_match import load_network
    from backend.mcp_server.data.constraint_network_match import (
        match_constraints_to_branches,
    )
    buses, branches = load_network()
    enriched = match_constraints_to_branches(constraints_df, branches, buses)
"""
from __future__ import annotations

import logging
import re
from typing import Optional

import pandas as pd

from backend.mcp_server.data.network_match import (
    _extract_xfmr_ckt_id,
    _normalize_station,
    list_neighbors,
    load_network,
)

logger = logging.getLogger(__name__)


# ─── Parsers ─────────────────────────────────────────────────────────────────

# DA underscore-coded LINE: ``<station>_<kv>KV<code>_<seq>_LN`` where station
# may contain underscores (BUTL_APS, MOUN_UGI, CARL_PN). The ``<kv>KV`` marker
# is the anchor — everything before is station, everything after up to
# ``_<seq>_LN`` is the (cryptic) endpoint code.
_RE_DA_LINE = re.compile(
    r"^(?P<station>[A-Z0-9_]+?)_(?P<kv>\d+)KV(?P<code>[A-Z0-9.\-]+?)_\d+_LN$"
)

# DA XFMR: ``<station>_T<n>_<kv><code>_XF``. Voltage is 2-3 digits (69/115/
# 138/230/345/500/765) — anchoring prevents the greedy ``\d+`` from eating
# trailing digits like the ``500-4`` xfmr code in ``CONASTON_T1_500500-4_XF``.
_RE_DA_XFMR = re.compile(
    r"^(?P<station>[A-Z0-9_]+?)_T(?P<txnum>\d+)_(?P<kv>\d{2,3})(?P<code>[A-Z0-9.\-]+?)_XF$"
)

# RT EMS tail: ``<body> [<class>] <kv> KV`` — class letter is optional
# (breaker/disconnect rows like ``... B1Z1 DIS  138 KV`` have none). We
# intentionally leave the class letter inside ``body`` and strip it later
# so DIS/CB/XFORMER tokens aren't accidentally captured as class.
_RE_RT_TAIL = re.compile(
    r"^(?P<body>.+?)\s+(?P<kv>\d+)\s+KV\s*$"
)
# Single trailing class letter (A/B/C/H/L/T/etc.) preceded by 2+ spaces.
_RE_RT_TRAILING_CLASS = re.compile(r"\s{2,}[A-Z]\s*$")
# Body shapes:
#   "STA1-STA2 1234"        — line endpoint pair + line id
#   "STA1-STA2"             — line endpoint pair only
#   "STATION 500-4 XFORMER" — XFMR with station + bus pair + XFORMER tag
#   "STATION TX1 XFORMER"   — XFMR with station + label + XFORMER tag
#   "74 KEWAN B1Z5 DIS"     — bus disconnect at station
#   "LADYSMTH H1T575 CB"    — circuit breaker at station

# Prose l/o: ``<from>-<to> <kv>[ kV] l/o ...``. The ``kV`` token is optional
# — some samples are ``... 138 l/o ...`` with no kV marker.
_RE_PROSE_LO = re.compile(
    r"^(?P<from>.+?)\s*-\s*(?P<to>.+?)\s+(?P<kv>\d+)\s*(?:kV)?\s+l/o\b",
    re.IGNORECASE,
)

# Trailing PJM utility-suffix codes that don't correspond to PSS/E station
# names. Stripped before lookup if the leading-token match fails.
_UTILITY_SUFFIXES = {
    "APS", "PN", "UGI", "BC", "CO", "FE", "ME", "JC", "PE", "PEP",
    "DPL", "CIN", "CWLP", "DOM", "AEP", "PL",
}


def _normalize_constraint_station(name: Optional[str]) -> Optional[str]:
    """Take the meaningful station token from a constraint string.

    Skips leading 1-3 digit zone-prefix numbers like ``"94"`` in
    ``"94 HAURD"`` (PJM uses small numeric prefixes as zone IDs). Keeps
    purely-numeric stations like ``"19906"`` since 4+ digit tokens are
    typically real bus IDs serving as substation names. Strips known utility
    suffix codes (``_APS``, ``_PN``, ``_UGI``) — they're not part of the
    PSS/E station name.
    """
    if not name:
        return None
    s = str(name).strip().upper()
    if not s:
        return None
    parts = [p for p in re.split(r"[_\s]+", s) if p]
    if not parts:
        return None
    # Skip a leading short-numeric prefix when there's a real token after it
    if len(parts) > 1 and parts[0].isdigit() and len(parts[0]) <= 3:
        parts = parts[1:]
    head = parts[0]
    # If head is a known utility-suffix code (rare), prefer the next token
    if head in _UTILITY_SUFFIXES and len(parts) > 1:
        return parts[1]
    return head


def parse_constraint_facility(facility: str) -> dict:
    """Return a normalized record with parser dialect + matchable fields.

    Keys: ``dialect``, ``equipment_type``, ``voltage_kv``, ``from_station``,
    ``to_station``, ``single_station``, ``xfmr_label``, ``raw``, ``second_code``.

    ``second_code`` carries the cryptic abbreviation in the DA LINE format
    (e.g. ``"CNS-PEA"``) — used downstream to disambiguate the second
    endpoint by prefix match against PSS/E station names.
    """
    raw = facility or ""
    rec = {
        "dialect": "UNKNOWN",
        "equipment_type": None,
        "voltage_kv": None,
        "from_station": None,
        "to_station": None,
        "single_station": None,
        "xfmr_label": None,
        "second_code": None,
        "raw": raw,
    }

    if not raw or not raw.strip():
        return rec

    s = raw.strip()

    # 1) Prose l/o (test before DA-coded since both can have underscores
    #    embedded — but prose has spaces and "l/o")
    m = _RE_PROSE_LO.match(s)
    if m:
        rec["dialect"] = "PROSE_LO"
        rec["equipment_type"] = "LINE"
        rec["voltage_kv"] = int(m.group("kv"))
        rec["from_station"] = m.group("from").strip()
        rec["to_station"] = m.group("to").strip()
        return rec

    # 2) DA underscore-coded XFMR (test before LINE since both end in _LN/_XF)
    m = _RE_DA_XFMR.match(s)
    if m:
        rec["dialect"] = "DA_CODED"
        rec["equipment_type"] = "XFMR"
        rec["voltage_kv"] = int(m.group("kv"))
        rec["single_station"] = m.group("station")
        rec["xfmr_label"] = f"T{m.group('txnum')}"
        rec["second_code"] = m.group("code")
        return rec

    # 3) DA underscore-coded LINE
    m = _RE_DA_LINE.match(s)
    if m:
        rec["dialect"] = "DA_CODED"
        rec["equipment_type"] = "LINE"
        rec["voltage_kv"] = int(m.group("kv"))
        rec["from_station"] = m.group("station")
        rec["second_code"] = m.group("code")
        # The ``code`` is e.g. "CNS-PEA" or "126A". When it has a hyphen we
        # take the right side as the abbreviation for the to-station.
        if "-" in (m.group("code") or ""):
            rec["to_station"] = m.group("code").split("-", 1)[1]
        return rec

    # 4) RT EMS fixed-width — anchor on trailing " <kv> KV"
    m = _RE_RT_TAIL.match(s)
    if m:
        rec["dialect"] = "RT_EMS"
        rec["voltage_kv"] = int(m.group("kv"))
        body = m.group("body")
        # Strip trailing single-letter class (preceded by 2+ spaces) so DIS
        # / CB / XFORMER inside the body are preserved for downstream checks.
        body = _RE_RT_TRAILING_CLASS.sub("", body).strip()

        # XFMR: contains "XFORMER"
        if "XFORMER" in body.upper():
            rec["equipment_type"] = "XFMR"
            # Take first whitespace token as station
            tok = body.split()
            rec["single_station"] = tok[0] if tok else None
            label = _extract_xfmr_ckt_id(body)
            if label:
                rec["xfmr_label"] = f"T{label}"
            return rec

        # CB / DIS: single-station device at substation. Strip the trailing
        # device-id + DIS/CB tokens; keep the leading station tokens (which
        # may include a short numeric zone prefix like ``74 KEWAN``).
        upper = body.upper()
        if " CB" in f" {upper} " or " DIS" in f" {upper} ":
            rec["equipment_type"] = "PS"
            # Drop tail tokens including DIS/CB so the leading tokens are
            # available for the normalizer's zone-prefix skip.
            tokens = body.split()
            keep = []
            for t in tokens:
                if t.upper() in ("DIS", "CB"):
                    break
                keep.append(t)
            # Drop the device-id token (last kept token) — it's bus-level junk
            if len(keep) >= 2:
                keep = keep[:-1]
            rec["single_station"] = " ".join(keep) if keep else None
            return rec

        # LINE: from-station can have internal spaces (``94 HAURD-11323``,
        # ``KELFORD -EARLEYS``); to-station is the next whitespace-delimited
        # token after the first dash.
        m2 = re.match(r"^(?P<from>.+?)\s*-\s*(?P<to>\S+)", body)
        if m2:
            rec["equipment_type"] = "LINE"
            rec["from_station"] = m2.group("from").strip()
            rec["to_station"] = m2.group("to")
            return rec

        # Fallback: single station
        rec["equipment_type"] = "PS"
        rec["single_station"] = body.split()[0] if body else None
        return rec

    # 5) Interface / zone / unknown — anything left without a kV anchor.
    # Common shapes: ``APSOUTH``, ``BCPEP``, ``APSOUTH contingency 22``.
    # If the string starts with a stand-alone alphabetic token and we
    # haven't matched any branch pattern, classify as INTERFACE.
    if re.match(r"^[A-Za-z][A-Za-z0-9_]{1,30}\b", s):
        rec["dialect"] = "INTERFACE"
        rec["equipment_type"] = "INTERFACE"
        rec["single_station"] = re.split(r"[\s_]+", s)[0]
        return rec

    return rec


# ─── Matcher ─────────────────────────────────────────────────────────────────


def _strip_trailing_digits(s: Optional[str]) -> Optional[str]:
    """``MUNSTER2`` → ``MUNSTER``; ``CONASTONE`` unchanged. Empty if all-digit."""
    if not s:
        return None
    out = re.sub(r"\d+$", "", s)
    return out if out else s


def _stations_match(constraint_token: str, psse_token: str, *, min_prefix: int = 4) -> bool:
    """Flexible station-name comparison.

    True if either token equals the other, or one is a prefix of the other
    with prefix length ≥ ``min_prefix``. Trailing digits are stripped from
    both before comparison (handles ``MUNSTER2`` vs ``MUNSTER``).
    """
    if not constraint_token or not psse_token:
        return False
    a = _strip_trailing_digits(constraint_token.upper()) or constraint_token.upper()
    b = _strip_trailing_digits(psse_token.upper()) or psse_token.upper()
    if a == b:
        return True
    short, long_ = (a, b) if len(a) <= len(b) else (b, a)
    return len(short) >= min_prefix and long_.startswith(short)


def _find_line_branches(
    leading_token: str,
    abbrev_token: Optional[str],
    voltage_int: int,
    branches_df: pd.DataFrame,
) -> list[int]:
    """Find PSS/E LINE branches matching a constraint's endpoints + kV.

    Multi-pass — returns the most specific non-empty result:

      Pass A: strict endpoint pair (both ``leading`` and ``abbrev`` match
              their respective PSS/E endpoints by ``_stations_match``).
      Pass B: leading station only — any LINE at this voltage with one
              endpoint matching ``leading_token``. Caller marks as
              ambiguous if multiple hits.

    The caller decides matched vs ambiguous from the return list length.
    """
    line_mask = (
        (branches_df["equipment_type"] == "LINE")
        & (branches_df["voltage_kv"].round().astype(int) == voltage_int)
    )
    candidate_idxs = branches_df.index[line_mask].tolist()

    # Pre-normalize PSS/E names for these candidates
    norm = {
        i: (
            _normalize_station(branches_df.loc[i, "from_name"]),
            _normalize_station(branches_df.loc[i, "to_name"]),
        )
        for i in candidate_idxs
    }

    # Pass A — strict pair (both endpoints satisfied). Allow ``min_prefix=3``
    # on the abbrev side: DA-coded constraints encode the second endpoint as
    # a 3-letter abbreviation (``PEA`` for PEACHBOTTOM, ``MAR`` for
    # MARYSVILLE) that would otherwise fail the default 4-char floor.
    if abbrev_token:
        pair_hits = [
            i for i in candidate_idxs
            if (
                (_stations_match(leading_token, norm[i][0])
                 and _stations_match(abbrev_token, norm[i][1], min_prefix=3))
                or (_stations_match(leading_token, norm[i][1])
                    and _stations_match(abbrev_token, norm[i][0], min_prefix=3))
            )
        ]
        if pair_hits:
            return pair_hits

    # Pass B — leading station only (anything at this kV touching leading)
    leading_hits = [
        i for i in candidate_idxs
        if _stations_match(leading_token, norm[i][0])
        or _stations_match(leading_token, norm[i][1])
    ]
    return leading_hits


def _find_xfmr_branches(
    station_token: str,
    voltage_int: int,
    branches_df: pd.DataFrame,
) -> list[int]:
    """Find PSS/E XFMR branches at the given station + voltage."""
    xfmr_mask = (
        (branches_df["equipment_type"] == "XFMR")
        & (branches_df["voltage_kv"].round().astype(int) == voltage_int)
    )
    candidate_idxs = branches_df.index[xfmr_mask].tolist()
    return [
        i for i in candidate_idxs
        if (
            _stations_match(station_token, _normalize_station(branches_df.loc[i, "from_name"]))
            or _stations_match(station_token, _normalize_station(branches_df.loc[i, "to_name"]))
        )
    ]


def match_constraints_to_branches(
    constraints_df: pd.DataFrame,
    branches_df: Optional[pd.DataFrame] = None,
    buses_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Return ``constraints_df`` enriched with PSS/E match metadata.

    Required input columns: ``constraint_name`` (PJM monitored_facility).

    Added columns:
      parser_dialect, parsed_equipment_type, parsed_voltage_kv,
      parsed_from_station, parsed_to_station, parsed_single_station,
      from_bus_psse, to_bus_psse, rating_mva, network_match_status,
      neighbor_count.

    ``network_match_status`` ∈ {matched, ambiguous, unmatched, interface}.
    Interface-class constraints (APSOUTH, BCPEP, etc.) get ``interface``
    and no bus IDs — they don't correspond to a single PSS/E branch.
    """
    if branches_df is None or buses_df is None:
        buses_df, branches_df = load_network()

    df = constraints_df.copy().reset_index(drop=True)
    matches: list[dict] = []

    for _, row in df.iterrows():
        facility = row.get("constraint_name") or ""
        parsed = parse_constraint_facility(facility)

        result = {
            "parser_dialect": parsed["dialect"],
            "parsed_equipment_type": parsed["equipment_type"],
            "parsed_voltage_kv": parsed["voltage_kv"],
            "parsed_from_station": parsed["from_station"],
            "parsed_to_station": parsed["to_station"],
            "parsed_single_station": parsed["single_station"],
            "from_bus_psse": None,
            "to_bus_psse": None,
            "rating_mva": None,
            "network_match_status": "unmatched",
        }

        equip = parsed["equipment_type"]
        kv = parsed["voltage_kv"]

        if equip == "INTERFACE":
            result["network_match_status"] = "interface"
            matches.append(result)
            continue

        if equip is None or kv is None:
            matches.append(result)
            continue

        kv_int = int(kv)
        hits: list[int] = []

        f_norm = _normalize_constraint_station(parsed["from_station"])
        t_norm = _normalize_constraint_station(parsed["to_station"])
        s_norm = _normalize_constraint_station(parsed["single_station"])

        if equip == "LINE" and f_norm:
            # Two-pass fuzzy: strict pair (both endpoints satisfied) first,
            # then leading-station-only (caller decides ambiguous from count).
            hits = _find_line_branches(f_norm, t_norm, kv_int, branches_df)
        elif equip in ("XFMR", "PS") and s_norm:
            hits = _find_xfmr_branches(s_norm, kv_int, branches_df)

        # XFMR disambiguation by transformer label (same logic as outage matcher)
        if hits and len(hits) > 1 and equip in ("XFMR", "PS"):
            target_label = parsed["xfmr_label"] or _extract_xfmr_ckt_id(facility)
            if target_label:
                num = _extract_xfmr_ckt_id(target_label)
                if num:
                    label_matches = [
                        h for h in hits
                        if _extract_xfmr_ckt_id(branches_df.loc[h, "name"]) == num
                    ]
                    if len(label_matches) == 1:
                        hits = label_matches

        if hits:
            br = branches_df.loc[hits[0]]
            result.update(
                from_bus_psse=int(br["from_bus"]),
                to_bus_psse=int(br["to_bus"]),
                rating_mva=float(br["rating_mva"]) if pd.notna(br["rating_mva"]) else None,
                network_match_status="matched" if len(hits) == 1 else "ambiguous",
            )

        matches.append(result)

    enriched = pd.concat(
        [df, pd.DataFrame(matches, index=df.index)], axis=1,
    )

    # 1-hop neighbor count (only for matched / ambiguous)
    def _count(row):
        fb, tb = row.get("from_bus_psse"), row.get("to_bus_psse")
        if pd.isna(fb) or pd.isna(tb):
            return None
        fb_i, tb_i = int(fb), int(tb)
        endpoints = {fb_i, tb_i}
        touches = (
            branches_df["from_bus"].isin(endpoints)
            | branches_df["to_bus"].isin(endpoints)
        )
        is_self_orig = (branches_df["from_bus"] == fb_i) & (branches_df["to_bus"] == tb_i)
        is_self_rev = (branches_df["from_bus"] == tb_i) & (branches_df["to_bus"] == fb_i)
        return int((touches & ~is_self_orig & ~is_self_rev).sum())

    enriched["neighbor_count"] = enriched.apply(_count, axis=1)

    return enriched


# Re-export for convenience
__all__ = [
    "parse_constraint_facility",
    "match_constraints_to_branches",
    "list_neighbors",
    "load_network",
]


if __name__ == "__main__":
    # Smoke test against a few representative strings
    samples = [
        "02LYONS_138KV02L-02F1_1_LN",
        "BURMA_115KVBUR-PIN_1_LN",
        "CONASTON_500KVCNS-PEA_1_LN",
        "CARL_PN_115KVCAR-GAR_1_LN",
        "BUTL_APS_138KVBUT-KAR_1_LN",
        "19906_138KV199-KEN6_1_LN",
        "KELFORD_115KV126A_1_LN",
        "BURMA_T1_138T1_XF",
        "CLOUD_T2_230TX1_XF",
        "CONASTON_T1_500500-4_XF",
        "BRIGHTON_T1_500NO.1TR_XF",
        "94 HAURD-11323    11323     A  138 KV",
        "BURNHAM-MUNSTER2  NIP  TIE  A  345 KV",
        "CONASTON-PEACHBOT 5012      B  500 KV",
        "CLOUD TX1 XFORMER           L  115 KV",
        "CONASTON 500-4    XFORMER   H  500 KV",
        "74 KEWAN B1Z1 DIS              138 KV",
        "LADYSMTH H1T575       CB       500 KV",
        "Batesville-Hubble 138 l/o Tanners Crk-Miami Fort 345",
        "Snyder - Sullivan 345 kV l/o Sidney - Bunsonville 345 kV",
        "APSOUTH",
        "APSOUTH contingency 22",
        "BCPEP",
    ]
    for raw in samples:
        rec = parse_constraint_facility(raw)
        print(
            f"{rec['dialect']:<10} {rec['equipment_type'] or '?':<10} "
            f"{rec['voltage_kv'] or '?':<5} "
            f"from={rec['from_station']!s:<20} to={rec['to_station']!s:<15} "
            f"single={rec['single_station']!s:<15} | {raw}"
        )
