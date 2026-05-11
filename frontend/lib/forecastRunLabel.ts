// Per-model run-picker label. Pure formatter — used by both client and
// server components, so no "server-only" import. Reads the projected
// metadata from ForecastRunListEntry; no DB access.
//
// Examples:
//   KNN: "13:34 · weekday · 40 analogs"
//   ICE: "13:34 · scale 0.95 · VWAP $42.36"
//   ICE (anchor not applied): "13:34 · raw (no ICE anchor)"

import type { ForecastRunListEntry, ModelFamily } from "@/types/forecast";

function timeOnly(localIso: string): string {
  // localIso is naive ISO (YYYY-MM-DDTHH:MM:SS) — slice avoids JS Date
  // converting based on the runtime tz, which would lie about MST.
  const t = localIso.slice(11, 16);
  return t || localIso;
}

function fmtMoney(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return `$${v.toFixed(2)}`;
}

function fmtScale(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return v.toFixed(2);
}

export function labelForRun(entry: ForecastRunListEntry): string {
  const time = timeOnly(entry.created_at_local);
  switch (entry.model_family as ModelFamily) {
    case "like_day":
      return labelForKnnRun(entry, time);
    case "baseline":
      return labelForIceRun(entry, time);
    default:
      return `${time} · ${entry.run_id.slice(0, 8)}`;
  }
}

function labelForKnnRun(entry: ForecastRunListEntry, time: string): string {
  const parts: string[] = [time];
  if (entry.metadata.day_type) parts.push(entry.metadata.day_type);
  if (entry.metadata.n_unique_analog_dates != null) {
    parts.push(`${entry.metadata.n_unique_analog_dates} analogs`);
  }
  return parts.join(" · ");
}

function labelForIceRun(entry: ForecastRunListEntry, time: string): string {
  const m = entry.metadata;
  if (m.ice_applied === false) {
    return `${time} · raw (no ICE anchor)`;
  }
  const parts: string[] = [time];
  if (m.ice_shared_scale != null) parts.push(`scale ${fmtScale(m.ice_shared_scale)}`);
  if (m.ice_vwap != null) parts.push(`VWAP ${fmtMoney(m.ice_vwap)}`);
  return parts.join(" · ");
}
