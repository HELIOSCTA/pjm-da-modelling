// Codified chart + table tokens. Per frontend-styling skill: hex literals
// live here, not in components. Anything chart-adjacent imports from this
// module so re-skinning is a single-file change.

// Axis / surface
export const TICK_COLOR = "#94a3b8"; // slate-400
export const GRID_COLOR = "#1f2937"; // gray-800
export const AXIS_COLOR = "#475569"; // slate-600

// Tooltip surface
export const TOOLTIP_BG = "#0f1117"; // matches --background
export const TOOLTIP_BORDER = "#374151"; // gray-700
export const TOOLTIP_FG = "#e5e7eb"; // matches --foreground

// Plot palette mirrors the table row colors so chart and tables read as
// one visual. Hue mapping:
//   P10 / P25 / P75 / P90 — orange family (matches band rows in the
//     table, which are font-bold orange-500 / orange-300).
//   Forecast (point) — sky family (matches the Forecast row sky-300).
//   P50 (median)    — gray (matches the italic gray-300 P50 row).
//   Actual          — purple family (matches the bold purple-400 Actual row).
export const BAND_OUTER_FILL = "#f97316"; // orange-500 — P10..P90 envelope
export const BAND_OUTER_OPACITY = 0.18;
export const BAND_INNER_FILL = "#fb923c"; // orange-400 — P25..P75 IQR
export const BAND_INNER_OPACITY = 0.30;
export const FORECAST_LINE = "#7dd3fc"; // sky-300
export const FORECAST_LINE_WIDTH = 2;
export const MEDIAN_LINE = "#9ca3af"; // gray-400 — distinct from forecast
export const MEDIAN_LINE_WIDTH = 1.5;

// Actual realised line (purple, dashed) — matches the table Actual row.
export const ACTUAL_LINE = "#c084fc"; // purple-400
export const ACTUAL_LINE_WIDTH = 2;
export const ACTUAL_DASH = "4 3";

// Compare-tab model overlays — one color per model series, in registry
// order (see COMPARE_MODELS in app/forecast/_lib/tabs.ts). If there are
// ever more models than colors, callers cycle modulo the array length.
// Actual stays ACTUAL_LINE / ACTUAL_DASH (purple dashed), distinct from
// every forecast series. Index 0 (sky-300) matches FORECAST_LINE and the
// Like-Day KNN tab; index 1 (orange-400) is the ICE-anchored series
// (this replaces the inline ICE_LINE that used to live in ShapeChart).
export const COMPARE_SERIES_COLORS = [
  "#7dd3fc", // sky-300
  "#fb923c", // orange-400
  "#34d399", // emerald-400
  "#fcd34d", // amber-300
  "#f472b6", // pink-400
] as const;

// PJM OnPeak block = HE8 .. HE23 inclusive. Centralised so the chart
// reference area and the table column shading agree.
export const ONPEAK_HE_START = 8;
export const ONPEAK_HE_END = 23;
export const ONPEAK_SHADE_FILL = "#1f2937"; // gray-800
export const ONPEAK_SHADE_OPACITY = 0.45;

// Shared column geometry — single source of truth for chart-table alignment.
// Table columns: 1 Band col + 24 HE cols + 3 block cols = 28. Band gets a
// slightly wider share so labels like "Forecast" fit; the remaining 27 cols
// each get an equal slice of the 94% remainder.
export const TABLE_BAND_COL_FRACTION = 0.06; // 6% — leftmost label col
export const TABLE_NARROW_COL_FRACTION = 0.94 / 27; // ≈ 3.481% — applies to each HE + each block
export const TABLE_HE_AREA_FRACTION = TABLE_NARROW_COL_FRACTION * 24; // ≈ 0.8356
export const TABLE_BLOCKS_AREA_FRACTION = TABLE_NARROW_COL_FRACTION * 3; // ≈ 0.1044
