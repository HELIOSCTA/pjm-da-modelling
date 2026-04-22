"""Send notifications via Slack with dedup via pipeline_runs table."""

import logging

import requests

from backend import secrets
from backend.utils import azure_postgresql_utils as azure_postgresql

logger = logging.getLogger("utils.notification_utils")


# ── Dedup ────────────────────────────────────────────────────────────────────


def already_notified(pipeline_name: str, target_date: str) -> bool:
    """Check if a notification was already sent for this pipeline + date."""
    df = azure_postgresql.pull_from_db(
        query=(
            f"SELECT 1 FROM logging.pipeline_runs "
            f"WHERE pipeline_name = '{pipeline_name}' "
            f"  AND event_type = 'SLACK_SENT' "
            f"  AND metadata::jsonb->>'target_date' = '{target_date}' "
            f"LIMIT 1"
        )
    )
    return df is not None and not df.empty


# ── Slack ────────────────────────────────────────────────────────────────────

SEVERITY_LABEL = {
    "success": "OK",
    "warning": "WARNING",
    "error": "ERROR",
    "info": "INFO",
}


def send_slack_notification(
    message: str,
    *,
    severity: str = "info",
    pipeline: str | None = None,
    fields: dict[str, str] | None = None,
) -> None:
    """Send a structured Slack webhook message using Block Kit.

    Args:
        message:  Main notification text.
        severity: One of "success", "warning", "error", "info".
        pipeline: Pipeline name shown in the header (optional).
        fields:   Extra key/value pairs rendered as a two-column section.
    """
    webhook_url = secrets.SLACK_DEFAULT_WEBHOOK_URL
    if not webhook_url:
        raise ValueError("SLACK_DEFAULT_WEBHOOK_URL is not set")

    label = SEVERITY_LABEL.get(severity, "INFO")
    header = pipeline or "Alert"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header, "emoji": False},
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"*Status:* {label}"},
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": message},
        },
    ]

    if fields:
        blocks.append({
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*{k}:*\n{v}"}
                for k, v in fields.items()
            ],
        })

    blocks.append({"type": "divider"})

    payload = {"blocks": blocks, "text": f"[{label}] {header}"}
    resp = requests.post(webhook_url, json=payload, timeout=10)
    resp.raise_for_status()
    logger.info(f"Slack notification sent: {message[:80]}")
