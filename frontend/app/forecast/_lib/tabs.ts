// Single source of truth for the tab nav. Each tab maps to a
// model_name (the publish identity used by the Postgres reader).

import type { ModelFamily } from "@/types/forecast";

export type TabKey = "like-day" | "ice-anchored" | "compare";

export interface TabDef {
  key: TabKey;
  label: string;
  href: string;
  // Per-model tabs map to one model_name. The compare tab spans both
  // models, so it has no single modelName — read MODEL_NAMES instead.
  modelName: string | null;
}

export const MODEL_NAMES = {
  knn: "pjm_rto_hourly",
  ice: "baseline_meteo_da_price_ice_anchored",
} as const;

export const TAB_DEFS: readonly TabDef[] = [
  {
    key: "like-day",
    label: "Like-Day KNN",
    href: "/forecast/like-day",
    modelName: MODEL_NAMES.knn,
  },
  {
    key: "ice-anchored",
    label: "ICE-Anchored Meteo",
    href: "/forecast/ice-anchored",
    modelName: MODEL_NAMES.ice,
  },
  {
    key: "compare",
    label: "Compare models",
    href: "/forecast/compare",
    modelName: null,
  },
] as const;

export const DEFAULT_TAB: TabKey = "like-day";

// ── Compare-tab model registry ─────────────────────────────────────────────
// Drives /forecast/compare: the page iterates this list for the per-model
// fetch bundle, the run pickers (URL param `run_<key>`), the chart series,
// and the table rows. Adding a model to the comparison is ONE entry here —
// no per-model wiring anywhere else. Series colors are assigned by index
// from COMPARE_SERIES_COLORS in render order (cycled if the list grows).
//
// TODO: a vintage-vs-vintage view (lead-1 vs lead-2 vs lead-3 runs for one
// model) and a forecast-vs-actual scorecard (per-model error leaderboard
// across recent target dates) are separate, future comparison views — not
// this registry.

export interface CompareModel {
  /** short slug — also the URL run-picker param suffix (`run_<key>`) */
  key: string;
  /** legend / table-row label */
  label: string;
  /** publish identity in pjm_model_outputs.forecast_runs */
  modelName: string;
  /** payload family — selects the type guard / hourly accessor */
  family: ModelFamily;
}

export const COMPARE_MODELS: readonly CompareModel[] = [
  {
    key: "knn",
    label: "Like-Day KNN",
    modelName: MODEL_NAMES.knn,
    family: "like_day",
  },
  {
    key: "ice",
    label: "ICE-Anchored Meteo",
    modelName: MODEL_NAMES.ice,
    family: "baseline",
  },
];
