"""PJM DA HRL LMP notifications — dedup + Slack alerting."""

import logging
from pathlib import Path

from backend.utils import azure_postgresql_utils as azure_postgresql, pipeline_run_logger
from backend.utils.notification_utils import (
    already_notified,
    send_slack_notification,
)

logger = logging.getLogger("pjm_da_hrl_notifications")

SQL_DIR = Path(__file__).parent / "sql"

ONPEAK_START = 8
ONPEAK_END = 23


def get_da_lmp_summary(target_date: str) -> str | None:
    """Query WESTERN HUB DA LMPs for target_date and return onpeak/offpeak/peak hour."""
    query = (SQL_DIR / "da_lmp_summary.sql").read_text().format(target_date=target_date)
    df = azure_postgresql.pull_from_db(query=query)
    if df is None or df.empty:
        logger.warning(f"No DA LMP data for {target_date}, skipping summary")
        return None

    df["hour_ending"] = df["datetime_beginning_ept"].dt.hour + 1

    onpeak = df[df["hour_ending"].between(ONPEAK_START, ONPEAK_END)]
    offpeak = df[~df["hour_ending"].between(ONPEAK_START, ONPEAK_END)]
    peak_row = df.loc[df["total_lmp_da"].idxmax()]

    onpeak_avg = onpeak["total_lmp_da"].mean()
    offpeak_avg = offpeak["total_lmp_da"].mean()
    flat_avg = df["total_lmp_da"].mean()
    peak_he = int(peak_row["hour_ending"])
    peak_price = peak_row["total_lmp_da"]

    return (
        f"```\n"
        f"{target_date}\n"
        f"------\n"
        f"OnPeak:  ${onpeak_avg:.2f}\n"
        f"OffPeak: ${offpeak_avg:.2f}\n"
        f"Flat:    ${flat_avg:.2f}\n"
        f"\n"
        f"Peak Hour (HE {peak_he}): ${peak_price:.2f}\n"
        f"```"
    )


def notify_da_lmps(target_date: str) -> None:
    """Send PJM DA LMP availability notification with dedup."""
    pipeline_name = "da_hrl_lmps"
    if already_notified(pipeline_name, target_date):
        return

    summary = get_da_lmp_summary(target_date)

    send_slack_notification(
        message=summary or f"PJM DA LMPs available for *{target_date}*",
        severity="success",
        pipeline="PJM DA HRL LMPs",
    )

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=pipeline_name, source="power",
    )
    run.log_notification(
        channel="slack",
        recipient="#helioscta-alerts",
        metadata={"target_date": target_date},
    )
