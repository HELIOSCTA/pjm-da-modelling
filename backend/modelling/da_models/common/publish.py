"""Single source of truth for the ``pjm_model_outputs.forecast_runs`` upsert.

All forecaster families publish through this function -- see
``modelling/CLAUDE.md``. Each family owns ``build_payload`` and
``extract_onpeak_forecast`` in its own ``publish.py``; this module owns the
row layout and delegates the DDL + write to
``utils.azure_postgresql_utils.upsert_to_azure_postgresql`` -- which
``CREATE SCHEMA / TABLE IF NOT EXISTS`` on first call, COPYs the row into a
temp table, and upserts on the primary key. (That helper also appends its
own ``created_at`` / ``updated_at`` audit columns to the table.)

``run_date`` is the *vintage* of the forecast -- the date it was produced,
not the date it delivers (``target_date``). For a "delivery is tomorrow"
run they're a day apart; once balance-of-week runs land, ``target_date -
run_date`` is the lead, and ``WHERE target_date = ? ORDER BY run_date``
shows a fixed delivery day's forecast evolving across vintages. It is a
plain column, not part of the PK -- ``run_id`` already makes rows unique,
so every run is kept and "latest" stays derived.

Run-creation timestamps live in the ``payload`` jsonb (``created_at_utc`` /
``created_at_local``); the table's row timestamps are the helper's
``created_at`` / ``updated_at``.

Row layout written here (one row per run, identified by ``run_id``)::

    pjm_model_outputs.forecast_runs (
        model_family                  text,
        model_name                    text,
        run_date                      date,
        target_date                   date,
        da_lmp_total_onpeak_forecast  numeric(10,2),
        payload                       jsonb,
        run_id                        uuid,
        created_at                    timestamptz,  -- added by the helper
        updated_at                    timestamptz,  -- added by the helper
        PRIMARY KEY (model_name, target_date, run_id)
    )
"""

from __future__ import annotations

import json
import logging
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)

_SCHEMA = "pjm_model_outputs"
_TABLE = "forecast_runs"
_COLUMNS: list[str] = [
    "model_family",
    "model_name",
    "run_date",
    "target_date",
    "da_lmp_total_onpeak_forecast",
    "payload",
    "run_id",
]
# Explicit SQL types -- otherwise the helper's create-table step infers them
# from the first row and would emit VARCHAR for ``payload`` and FLOAT for the
# OnPeak number instead of ``jsonb`` / ``numeric(10,2)``. Order matches _COLUMNS.
_DATA_TYPES: list[str] = [
    "TEXT",
    "TEXT",
    "DATE",
    "DATE",
    "NUMERIC(10,2)",
    "JSONB",
    "UUID",
]
_PRIMARY_KEY: list[str] = ["model_name", "target_date", "run_id"]


def publish_forecast_run(
    *,
    model_name: str,
    model_family: str,
    target_date: date,
    run_date: date,
    run_id: str,
    payload: dict,
    da_lmp_total_onpeak_forecast: float | None,
) -> tuple[str, date, str]:
    """Upsert one forecast run into ``pjm_model_outputs.forecast_runs``.

    DDL + write are delegated to ``upsert_to_azure_postgresql``: the
    ``pjm_model_outputs`` schema and the table are created on the first
    call, then each call upserts a single row keyed by
    ``(model_name, target_date, run_id)``. Re-invoking with the same
    ``run_id`` overwrites the row (idempotent retry); a fresh ``run_id``
    appends a new one.

    ``run_date`` is the forecast vintage (typically ``target_date - 1``).
    ``payload`` is stored as jsonb. Returns ``(model_name, target_date,
    run_id)`` for caller logging.
    """
    import backend.credentials as credentials  # noqa: PLC0415
    from backend.utils.azure_postgresql_utils import upsert_to_azure_postgresql  # noqa: PLC0415

    row = pd.DataFrame(
        [
            {
                "model_family": model_family,
                "model_name": model_name,
                "run_date": run_date,
                "target_date": target_date,
                "da_lmp_total_onpeak_forecast": da_lmp_total_onpeak_forecast,
                "payload": json.dumps(payload, default=str),
                "run_id": run_id,
            }
        ],
        columns=_COLUMNS,
    )

    upsert_to_azure_postgresql(
        schema=_SCHEMA,
        table_name=_TABLE,
        df=row,
        columns=_COLUMNS,
        primary_key=_PRIMARY_KEY,
        data_types=_DATA_TYPES,
        database=credentials.AZURE_POSTGRESQL_DB_NAME,
    )

    logger.info(
        "Published forecast: %s / target=%s / vintage=%s / %s (onpeak=%s)",
        model_name,
        target_date,
        run_date,
        run_id,
        da_lmp_total_onpeak_forecast,
    )
    return model_name, target_date, run_id
