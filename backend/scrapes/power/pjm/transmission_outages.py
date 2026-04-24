"""
PJM Transmission Outages (eDART linesout.txt)

Scrapes the PJM Transmission Facilities Outage List (TFOL) from eDART,
parses the fixed-width TXT report, and upserts a daily snapshot to PostgreSQL.

Source: https://edart.pjm.com/reports/linesout.txt (served as ZIP)
Docs:   https://www.pjm.com/markets-and-operations/etools/oasis/system-information/outage-info.aspx

Orchestration: Scheduled (once daily)
  - Upstream is a static pull endpoint with no webhook/event mechanism.
  - Publish cadence is ~daily; downstream use is daily reporting.
  - Score: Scheduled 32 vs Event-driven 18 (see Section 8 rubric).
"""

import io
import re
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from backend import credentials
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# ── Config ───────────────────────────────────────────────────────────────────

API_SCRAPE_NAME = "transmission_outages"
LINESOUT_URL = "https://edart.pjm.com/reports/linesout.txt"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

# ── Regex patterns ───────────────────────────────────────────────────────────

TIMESTAMP_RE = re.compile(r"TIMESTAMP:(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2})")

# First line of a scheduled-outage record (has item number + ticket)
RECORD_HEADER_RE = re.compile(
    r"^\s*(\d+)\s+"  # item_number
    r"(\d+)\s+"  # ticket_id
    r"(\S+)\s+"  # zone
    r"(.+?)\s+"  # facility_name (lazy, anchored by date)
    r"(\d{2}-[A-Z]{3}-\d{4}\s+\d{4})\s+"  # start_datetime
    r"(\d{2}-[A-Z]{3}-\d{4}\s+\d{4})\s+"  # end_datetime
    r"([OC])\s+"  # status (Open / Closed)
    r"(\S+)\s+"  # outage_state
    r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})"  # last_revised
)

# Continuation line: additional equipment on the same ticket
EQUIP_CONT_RE = re.compile(
    r"^\s{10,}"  # leading whitespace (no item/ticket)
    r"(\S+)\s+"  # zone
    r"\S+\s+"  # equipment_type
    r".+?"  # facility detail
    r"(\d{2}-[A-Z]{3}-\d{4}\s+\d{4})\s+"  # start_datetime
    r"(\d{2}-[A-Z]{3}-\d{4}\s+\d{4})\s+"  # end_datetime
    r"([OC])"  # status
)

# Equipment type / station / voltage from the facility field
# Station names may contain spaces (e.g. "18 WILL", "51 MC CO");
# voltage may be decimal (e.g. "13.8 KV").
FACILITY_RE = re.compile(
    r"^(\S+)\s+"  # equipment_type  (BRKR, XFMR, LINE, CAP, LD ...)
    r"(.+?)\s+"  # station (lazy, anchored by voltage)
    r"(\d+(?:\.\d+)?)\s+KV"  # voltage_kv
)

# Parenthesised cause / log text
CAUSE_RE = re.compile(r"\(([^)]+)\)")

# Record delimiter
DELIMITER_RE = re.compile(r"^\+-----\+")

# Date-log line inside parentheses (starts with DD-MMM-YYYY)
DATE_LOG_RE = re.compile(r"^\d{2}-[A-Z]{3}-\d{4}")

# Status-history keywords
STATUS_KEYWORDS = {
    "Active", "Approved", "Received", "Submitted", "Withdrawn", "Denied", "Completed",
}


# ── Helpers ──────────────────────────────────────────────────────────────────


def _parse_pjm_datetime(dt_str: str) -> datetime | None:
    """Parse 'DD-MMM-YYYY HHMM' -> datetime."""
    try:
        return datetime.strptime(dt_str.strip(), "%d-%b-%Y %H%M")
    except (ValueError, AttributeError):
        return None


def _parse_revised_datetime(dt_str: str) -> datetime | None:
    """Parse 'MM/DD/YYYY HH:MM' -> datetime."""
    try:
        return datetime.strptime(dt_str.strip(), "%m/%d/%Y %H:%M")
    except (ValueError, AttributeError):
        return None


def _parse_trailing_metadata(line: str, match_end: int) -> dict:
    """Best-effort extraction of metadata after last_revised."""
    trailing = line[match_end:].strip().rstrip("|").strip()
    parts = [p for p in trailing.split() if p != "|"]

    meta = {
        "rtep": "",
        "availability": "",
        "risk": "",
        "approval_status": "",
        "on_time": "",
    }

    known_avail = {"Duration"}
    known_yesno = {"No", "Yes"}
    known_approval = {"Submitted", "Approved", "Received", "Withdrawn", "Denied"}

    idx = 0
    # RTEP (optional, appears before availability)
    if idx < len(parts) and parts[idx] not in known_avail and parts[idx] not in known_yesno:
        meta["rtep"] = parts[idx]
        idx += 1
    # availability
    if idx < len(parts) and parts[idx] in known_avail:
        meta["availability"] = parts[idx]
        idx += 1
    # risk
    if idx < len(parts) and parts[idx] in known_yesno:
        meta["risk"] = parts[idx]
        idx += 1
    # approval_status
    if idx < len(parts) and parts[idx] in known_approval:
        meta["approval_status"] = parts[idx]
        idx += 1
    # on_time
    if idx < len(parts) and parts[idx] in known_yesno:
        meta["on_time"] = parts[idx]
        idx += 1

    return meta


DURATION_KEYWORDS = {"Continuous"}


def _is_cause_line(text: str) -> bool:
    """Return True if parenthesised text is a cause/type (not a log entry)."""
    if not text:
        return False
    if DATE_LOG_RE.match(text):
        return False
    first_word = text.split()[0]
    if first_word in STATUS_KEYWORDS:
        return False
    if text.strip() in DURATION_KEYWORDS:
        return False
    return True


# ── Core pipeline functions ──────────────────────────────────────────────────


def _pull() -> str:
    """Download and extract linesout.txt from the PJM eDART ZIP endpoint."""
    logger.section("Downloading linesout.txt from eDART...")
    response = requests.get(LINESOUT_URL, timeout=120)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        names = zf.namelist()
        txt_name = next((n for n in names if n.endswith(".txt")), names[0])
        text = zf.read(txt_name).decode("utf-8", errors="replace")

    logger.success(f"Downloaded and extracted {txt_name} ({len(text):,} chars)")
    return text


def _format(text: str) -> pd.DataFrame:
    """Parse the fixed-width linesout.txt into a structured DataFrame."""
    lines = text.split("\n")

    # ── Scrape timestamp ─────────────────────────────────────────────────
    ts_match = TIMESTAMP_RE.search(lines[0]) if lines else None
    scrape_timestamp = (
        datetime.strptime(ts_match.group(1), "%m-%d-%Y %H:%M:%S")
        if ts_match
        else datetime.now()
    )
    scrape_date = scrape_timestamp.date()

    # ── Find section boundaries ──────────────────────────────────────────
    sched_start = None
    planned_start = None
    for i, line in enumerate(lines):
        if "SCHEDULED OUTAGES" in line and sched_start is None:
            sched_start = i
        if "PLANNED OUTAGES" in line and planned_start is None:
            planned_start = i

    if sched_start is None:
        logger.exception("Could not find SCHEDULED OUTAGES section")
        return pd.DataFrame()

    # ── Parse scheduled outages ──────────────────────────────────────────
    records = _parse_outage_section(
        lines, sched_start, planned_start or len(lines), "scheduled"
    )

    # Parse planned outages (often empty)
    if planned_start is not None:
        records.extend(
            _parse_outage_section(lines, planned_start, len(lines), "planned")
        )

    if not records:
        logger.section("No outage records found")
        return pd.DataFrame()

    # ── Build DataFrame ──────────────────────────────────────────────────
    df = pd.DataFrame(records)
    df["scrape_date"] = pd.Timestamp(scrape_date)
    df["scrape_timestamp"] = pd.Timestamp(scrape_timestamp)

    # Flatten causes list
    df["cause"] = df["causes"].apply(lambda x: "; ".join(x[:3]) if x else "")
    df.drop(columns=["causes"], inplace=True)

    # Ensure datetime columns
    for col in ("start_datetime", "end_datetime", "last_revised"):
        df[col] = pd.to_datetime(df[col], errors="coerce")

    logger.success(f"Parsed {len(df)} outage records")
    return df


def _parse_outage_section(
    lines: list[str], start: int, end: int, section: str
) -> list[dict]:
    """Parse one outage section (scheduled or planned) into record dicts."""
    records: list[dict] = []
    current: dict | None = None

    for i in range(start, end):
        line = lines[i]

        # Record delimiter
        if DELIMITER_RE.search(line):
            if current is not None:
                records.append(current)
                current = None
            continue

        # Try record header (first line of a ticket)
        hdr = RECORD_HEADER_RE.search(line)
        if hdr:
            facility = hdr.group(4).strip()
            fac_match = FACILITY_RE.match(facility)
            meta = _parse_trailing_metadata(line, hdr.end())

            current = {
                "ticket_id": int(hdr.group(2)),
                "item_number": int(hdr.group(1)),
                "zone": hdr.group(3).strip(),
                "facility_name": facility,
                "equipment_type": fac_match.group(1) if fac_match else "",
                "station": fac_match.group(2) if fac_match else "",
                "voltage_kv": float(fac_match.group(3)) if fac_match else None,
                "start_datetime": _parse_pjm_datetime(hdr.group(5)),
                "end_datetime": _parse_pjm_datetime(hdr.group(6)),
                "status": hdr.group(7).strip(),
                "outage_state": hdr.group(8).strip(),
                "last_revised": _parse_revised_datetime(hdr.group(9)),
                **meta,
                "equipment_count": 1,
                "causes": [],
                "section": section,
            }
            continue

        if current is None:
            continue

        stripped = line.strip()

        # Standalone cause / log line in parentheses
        if stripped.startswith("(") and stripped.endswith(")"):
            cause_text = stripped[1:-1].strip()
            if _is_cause_line(cause_text):
                current["causes"].append(cause_text)
            continue

        # Equipment continuation line (has zone + dates)
        eq = EQUIP_CONT_RE.search(line)
        if eq:
            current["equipment_count"] += 1
            # Check for inline cause after the status
            remainder = line[eq.end() :]
            cm = CAUSE_RE.search(remainder)
            if cm:
                cause_text = cm.group(1).strip()
                if _is_cause_line(cause_text):
                    current["causes"].append(cause_text)

    # Flush last record
    if current is not None:
        records.append(current)

    return records


def _upsert(
    df: pd.DataFrame,
    schema: str = "pjm",
    table_name: str = API_SCRAPE_NAME,
    primary_key: list = ["ticket_id"],
) -> None:
    """Upsert the current outage snapshot (latest state per ticket)."""
    data_types = azure_postgresql.infer_sql_data_types(df=df)

    azure_postgresql.upsert_to_azure_postgresql(
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=primary_key,
    )


# ── Entrypoint ───────────────────────────────────────────────────────────────


def main():
    """Orchestrate: pull -> format -> upsert."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="power",
        target_table=f"pjm.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)

        # Pull
        text = _pull()

        # Format / parse
        logger.section("Parsing outage data...")
        df = _format(text)

        if df.empty:
            logger.section("No outage data parsed, skipping upsert.")
            run.success(rows_processed=0)
            return df

        # Upsert current snapshot
        logger.section(f"Upserting {len(df)} outage records...")
        _upsert(df)

        run.success(rows_processed=len(df))

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logging_utils.close_logging()

    return df


if __name__ == "__main__":
    df = main()
