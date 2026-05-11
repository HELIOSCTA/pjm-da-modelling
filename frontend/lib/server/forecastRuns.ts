import "server-only";

import { query } from "./db";
import type {
  ForecastRunListEntry,
  ForecastRunPayload,
} from "@/types/forecast";

const TABLE = "pjm_model_outputs.forecast_runs";

// ── Picker list ─────────────────────────────────────────────────────────────

// Projects only what the labeler needs — the full payload is pulled
// lazily by readForecastRun once the user picks a row.
const LIST_SQL = `
  SELECT
    model_family,
    model_name,
    target_date::text                                         AS target_date,
    run_date::text                                            AS run_date,
    run_id::text                                               AS run_id,
    -- headline on-peak number, promoted onto the table so the picker /
    -- compare view can show it (and the cross-model spread) without
    -- cracking the payload jsonb. numeric(10,2) -> float8 so pg hands
    -- back a JS number, not a string.
    da_lmp_total_onpeak_forecast::float8                      AS da_lmp_total_onpeak_forecast,
    -- run-creation timestamps live in the payload jsonb; the table's
    -- created_at / updated_at are the upsert helper's row-write audit cols
    payload->>'created_at_utc'                                AS created_at_utc,
    payload->>'created_at_local'                              AS created_at_local,
    payload->>'day_type'                                      AS day_type,
    NULLIF(payload->>'n_unique_analog_dates','')::int         AS n_unique_analog_dates,
    NULLIF(payload->'ice_anchor'->>'vwap','')::float8         AS ice_vwap,
    NULLIF(payload->'ice_anchor'->>'shared_scale','')::float8 AS ice_shared_scale,
    payload->'ice_anchor'->>'anchor_label'                    AS ice_anchor_label,
    NULLIF(payload->'ice_anchor'->>'applied','')::boolean     AS ice_applied
  FROM ${TABLE}
  WHERE model_name = $1
    AND ($2::date IS NULL OR target_date = $2::date)
  ORDER BY target_date DESC, run_date DESC, created_at DESC
`;

interface ListRow {
  model_family: ForecastRunListEntry["model_family"];
  model_name: string;
  target_date: string;
  run_date: string;
  run_id: string;
  da_lmp_total_onpeak_forecast: number | null;
  created_at_utc: string;
  created_at_local: string;
  day_type: string | null;
  n_unique_analog_dates: number | null;
  ice_vwap: number | null;
  ice_shared_scale: number | null;
  ice_anchor_label: ForecastRunListEntry["metadata"]["ice_anchor_label"];
  ice_applied: boolean | null;
}

function rowToListEntry(r: ListRow): ForecastRunListEntry {
  return {
    model_family: r.model_family,
    model_name: r.model_name,
    target_date: r.target_date,
    run_date: r.run_date,
    run_id: r.run_id,
    da_lmp_total_onpeak_forecast: r.da_lmp_total_onpeak_forecast ?? null,
    created_at_utc: r.created_at_utc,
    created_at_local: r.created_at_local,
    metadata: {
      day_type: r.day_type ?? undefined,
      n_unique_analog_dates: r.n_unique_analog_dates ?? undefined,
      ice_vwap: r.ice_vwap,
      ice_shared_scale: r.ice_shared_scale,
      ice_anchor_label: r.ice_anchor_label,
      ice_applied: r.ice_applied ?? undefined,
    },
  };
}

export async function listForecastRuns(
  modelName: string,
  targetDate?: string,
): Promise<ForecastRunListEntry[]> {
  const rows = await query<ListRow>(LIST_SQL, [modelName, targetDate ?? null]);
  return rows.map(rowToListEntry);
}

// ── Available target dates (for date picker) ───────────────────────────────

export async function listAvailableTargetDates(
  modelName: string,
): Promise<string[]> {
  const rows = await query<{ target_date: string }>(
    `SELECT DISTINCT target_date::text AS target_date
     FROM ${TABLE}
     WHERE model_name = $1
     ORDER BY target_date DESC`,
    [modelName],
  );
  return rows.map((r) => r.target_date);
}

// ── Full payload (single run) ──────────────────────────────────────────────

export async function readForecastRun(
  modelName: string,
  targetDate: string,
  runId: string,
): Promise<ForecastRunPayload | null> {
  const rows = await query<{ payload: ForecastRunPayload }>(
    `SELECT payload FROM ${TABLE}
     WHERE model_name = $1 AND target_date = $2 AND run_id = $3
     LIMIT 1`,
    [modelName, targetDate, runId],
  );
  return rows[0]?.payload ?? null;
}

// ── Latest run for (model, date) ───────────────────────────────────────────

// "Latest" is derived — no pointer row: most recent run_date (vintage),
// then most recent created_at (row write) within that vintage.
export async function readLatestForecastRun(
  modelName: string,
  targetDate: string,
): Promise<ForecastRunPayload | null> {
  const rows = await query<{ payload: ForecastRunPayload }>(
    `SELECT payload FROM ${TABLE}
     WHERE model_name = $1 AND target_date = $2
     ORDER BY run_date DESC, created_at DESC
     LIMIT 1`,
    [modelName, targetDate],
  );
  return rows[0]?.payload ?? null;
}
