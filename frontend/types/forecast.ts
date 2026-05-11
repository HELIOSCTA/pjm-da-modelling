// Shape of the forecast run JSON published by the Python pipelines into
// pjm_model_outputs.forecast_runs. Schema source of truth lives in the
// publishers — keep this file in sync with:
//   - modelling/da_models/like_day_model_knn/pjm_rto_hourly/publish.py
//   - modelling/da_models/baseline_meteo_da_price/publish.py
//
// The runs table is a discriminated union keyed by `model_family`:
//   - "like_day"  -> KnnPayload (P10/P25/P50/P75/P90 + analog days)
//   - "baseline"  -> IcePayload (Det + ENS bands + ICE anchor + trades)
// Use the type guards at the bottom to narrow.

// ── Common fields ──────────────────────────────────────────────────────────

export type ModelFamily = "like_day" | "baseline";
export type BlockName = "OnPeak" | "OffPeak" | "Flat";

export interface ForecastRunCommon {
  model_family: ModelFamily;
  model_name: string;
  target_date: string; // YYYY-MM-DD (delivery date)
  run_date: string; // YYYY-MM-DD (forecast vintage; target_date - run_date = lead days)
  run_id: string;
  hub: string;
  created_at_utc: string; // ISO with Z
  created_at_local: string; // ISO without Z (naive MST/MDT wall-clock)
}

// ── KNN like-day payload (model_family = "like_day") ───────────────────────

export interface HourlyForecastEntry {
  hour_ending: number;
  point_forecast: number | null;
  q10: number | null;
  q25: number | null;
  q50: number | null;
  q75: number | null;
  q90: number | null;
  actual_lmp: number | null;
}

export type QuantileLabel = "P10" | "P25" | "P50" | "Forecast" | "P75" | "P90";

export interface BlockBandEntry {
  block: BlockName;
  quantile_label: QuantileLabel;
  value: number | null;
}

export interface AnalogEntry {
  rank: number;
  analog_date: string; // YYYY-MM-DD
  day_of_week: string; // 3-letter abbrev
  day_diff: number;
  weight_share: number;
  hes_contributed: number;
  da_onpk_lmp: number | null;
}

export interface KnnPayload extends ForecastRunCommon {
  model_family: "like_day";
  day_type: string;
  n_analogs: number;
  n_unique_analog_dates: number;
  hourly: HourlyForecastEntry[];
  blocks: BlockBandEntry[];
  analogs: AnalogEntry[];
}

// ── ICE-anchored Meteo payload (model_family = "baseline") ─────────────────

export interface IceHourlyEntry {
  hour_ending: number;
  point_forecast: number | null; // Det (scaled when anchor applied)
  ens_avg: number | null;
  ens_bottom: number | null;
  ens_top: number | null;
  members_p25: number | null;
  members_p75: number | null;
  actual_lmp: number | null;
}

export type IceSeriesLabel = "Det" | "ENS Avg" | "ENS Bottom" | "ENS Top";

export interface IceBlockEntry {
  series: IceSeriesLabel;
  block: BlockName;
  value: number | null;
}

export interface IceAnchor {
  symbol: string;
  cutoff_local: string | null;
  applied: boolean;
  shared_scale: number | null;
  anchor_label: IceSeriesLabel | null;
  vwap: number | null;
  volume: number | null;
  n_trades: number;
  n_excluded: number;
  last_price: number | null;
  last_time_local: string | null;
  implied_multipliers: Partial<Record<IceSeriesLabel, number>>;
}

export type IceTradeDirection = "Lift" | "Hit" | "Leg" | "Spread" | string;

export interface IceTradeEntry {
  exec_time_local: string | null;
  price: number | null;
  quantity: number | null;
  trade_direction: IceTradeDirection | null;
}

export interface IcePayload extends ForecastRunCommon {
  model_family: "baseline";
  lead_days: number | null;
  det_executed_local: string | null;
  ens_executed_local: string | null;
  ice_anchor: IceAnchor;
  hourly: IceHourlyEntry[];
  blocks: IceBlockEntry[];
  ice_trades: IceTradeEntry[];
}

// ── Discriminated union + guards ───────────────────────────────────────────

export type ForecastRunPayload = KnnPayload | IcePayload;

export function isKnnPayload(p: ForecastRunPayload): p is KnnPayload {
  return p.model_family === "like_day";
}

export function isIcePayload(p: ForecastRunPayload): p is IcePayload {
  return p.model_family === "baseline";
}

// ── Picker / list view ─────────────────────────────────────────────────────

// Trimmed projection for the run-picker dropdown. Carries enough metadata
// for the per-model labeler (see lib/forecastRunLabel.ts) to render a
// meaningful row without fetching the full payload.
export interface ForecastRunListEntry {
  model_family: ModelFamily;
  model_name: string;
  target_date: string;
  run_date: string;
  run_id: string;
  // headline on-peak (HE8–23 mean) point forecast, straight off the
  // forecast_runs column — null on older rows written before the column.
  da_lmp_total_onpeak_forecast: number | null;
  created_at_utc: string;
  created_at_local: string;
  // Per-family metadata projected from the jsonb. Keys present depend on
  // the family — labeler narrows on model_family before reading.
  metadata: {
    // KNN
    day_type?: string;
    n_unique_analog_dates?: number;
    // ICE
    ice_vwap?: number | null;
    ice_shared_scale?: number | null;
    ice_anchor_label?: IceSeriesLabel | null;
    ice_applied?: boolean;
  };
}
