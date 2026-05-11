import { ONPEAK_HE_END, ONPEAK_HE_START } from "@/lib/chartConstants";
import type { HourlyForecastEntry, BlockName } from "@/types/forecast";

export const HE_LIST = Array.from({ length: 24 }, (_, i) => i + 1);
export const ONPEAK_HES = HE_LIST.filter(
  (h) => h >= ONPEAK_HE_START && h <= ONPEAK_HE_END,
);
export const OFFPEAK_HES = HE_LIST.filter(
  (h) => h < ONPEAK_HE_START || h > ONPEAK_HE_END,
);

export const BLOCK_HES: Record<BlockName, number[]> = {
  OnPeak: ONPEAK_HES,
  OffPeak: OFFPEAK_HES,
  Flat: HE_LIST,
};

export function getHourlyByHE(
  hourly: HourlyForecastEntry[],
): Map<number, HourlyForecastEntry> {
  return new Map(hourly.map((h) => [h.hour_ending, h]));
}

export function meanOrNull(values: (number | null | undefined)[]): number | null {
  const xs = values.filter(
    (v): v is number => typeof v === "number" && Number.isFinite(v),
  );
  if (xs.length === 0) return null;
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

export function rmseOrNull(values: (number | null | undefined)[]): number | null {
  const xs = values.filter(
    (v): v is number => typeof v === "number" && Number.isFinite(v),
  );
  if (xs.length === 0) return null;
  const meanSq = xs.reduce((a, b) => a + b * b, 0) / xs.length;
  return Math.sqrt(meanSq);
}

export function widthAtHE(h: HourlyForecastEntry): number | null {
  return h.q10 !== null && h.q90 !== null ? h.q90 - h.q10 : null;
}

export function iqrAtHE(h: HourlyForecastEntry): number | null {
  return h.q25 !== null && h.q75 !== null ? h.q75 - h.q25 : null;
}

export function skewAtHE(h: HourlyForecastEntry): number | null {
  return h.q10 !== null && h.q50 !== null && h.q90 !== null
    ? h.q90 - h.q50 - (h.q50 - h.q10)
    : null;
}

export function errorAtHE(h: HourlyForecastEntry): number | null {
  return h.actual_lmp !== null && h.point_forecast !== null
    ? h.point_forecast - h.actual_lmp
    : null;
}

export function absErrorAtHE(h: HourlyForecastEntry): number | null {
  const e = errorAtHE(h);
  return e === null ? null : Math.abs(e);
}

export function mapeAtHE(h: HourlyForecastEntry): number | null {
  if (h.actual_lmp === null || h.point_forecast === null) return null;
  if (h.actual_lmp === 0) return null;
  return Math.abs((h.point_forecast - h.actual_lmp) / h.actual_lmp) * 100;
}

export function inBand80AtHE(h: HourlyForecastEntry): boolean | null {
  if (h.actual_lmp === null || h.q10 === null || h.q90 === null) return null;
  return h.q10 <= h.actual_lmp && h.actual_lmp <= h.q90;
}

const QUANTILE_LEVELS: { q: number; key: keyof HourlyForecastEntry }[] = [
  { q: 0.1, key: "q10" },
  { q: 0.25, key: "q25" },
  { q: 0.5, key: "q50" },
  { q: 0.75, key: "q75" },
  { q: 0.9, key: "q90" },
];

export function crpsAtHE(h: HourlyForecastEntry): number | null {
  if (h.actual_lmp === null) return null;
  const a = h.actual_lmp;
  const losses: number[] = [];
  for (const { q, key } of QUANTILE_LEVELS) {
    const p = h[key] as number | null;
    if (p === null) continue;
    const e = a - p;
    losses.push(Math.max(q * e, (q - 1.0) * e));
  }
  if (losses.length === 0) return null;
  return 2.0 * (losses.reduce((x, y) => x + y, 0) / losses.length);
}

export function deltaP50AtHE(
  hourly: HourlyForecastEntry[],
  he: number,
): number | null {
  if (he <= 1) return null;
  const map = getHourlyByHE(hourly);
  const cur = map.get(he);
  const prev = map.get(he - 1);
  if (!cur || !prev || cur.q50 === null || prev.q50 === null) return null;
  return cur.q50 - prev.q50;
}

export function aggregateByBlock(
  perHE: (number | null)[],
): Record<BlockName, number | null> {
  const sliceMean = (hes: number[]) =>
    meanOrNull(hes.map((he) => perHE[he - 1]));
  return {
    OnPeak: sliceMean(ONPEAK_HES),
    OffPeak: sliceMean(OFFPEAK_HES),
    Flat: sliceMean(HE_LIST),
  };
}

export function blockRMSE(
  hourly: HourlyForecastEntry[],
  block: BlockName,
): number | null {
  const map = getHourlyByHE(hourly);
  const errs = BLOCK_HES[block].map((he) => {
    const r = map.get(he);
    return r ? errorAtHE(r) : null;
  });
  return rmseOrNull(errs);
}

export function blockInBand80Coverage(
  hourly: HourlyForecastEntry[],
  block: BlockName,
): { covered: number; total: number } | null {
  const map = getHourlyByHE(hourly);
  let covered = 0;
  let total = 0;
  for (const he of BLOCK_HES[block]) {
    const h = map.get(he);
    if (!h) continue;
    const v = inBand80AtHE(h);
    if (v === null) continue;
    total += 1;
    if (v) covered += 1;
  }
  if (total === 0) return null;
  return { covered, total };
}

export function hasActuals(hourly: HourlyForecastEntry[]): boolean {
  return hourly.some((h) => h.actual_lmp !== null);
}

// ── Gradient coloring (mirrors printers._gradient_color) ────────────────
// Five buckets: dark green (best) → green → neutral → red → dark red (worst).
// Bucket thresholds match the Python printer so chart/table/terminal agree.
export type GradientKind = "low_is_good" | "abs_low_is_good";

export function gradientRange(values: (number | null)[]): {
  lo: number;
  hi: number;
} {
  const finite = values.filter(
    (v): v is number => v !== null && Number.isFinite(v),
  );
  if (finite.length === 0) return { lo: 0, hi: 0 };
  return { lo: Math.min(...finite), hi: Math.max(...finite) };
}

export function gradientCellClass(
  val: number | null,
  lo: number,
  hi: number,
  kind: GradientKind,
): string {
  if (val === null || !Number.isFinite(val)) return "";
  let norm: number;
  if (kind === "abs_low_is_good") {
    const maxAbs = Math.max(Math.abs(lo), Math.abs(hi));
    if (maxAbs <= 0) return "";
    norm = Math.abs(val) / maxAbs;
  } else {
    if (hi <= lo) return "";
    norm = (val - lo) / (hi - lo);
  }
  // Terminal-style text-colour shading, lifted for screen contrast.
  // Threshold buckets match printers._gradient_color (0.10 / 0.20 / 0.35 / 0.65).
  // The terminal uses ANSI 22 / 28 / 124 / 88 (dark green / green / red / dark red)
  // — those hexes sit around #005f00 / #870000 which read fine on a true-black
  // terminal but get lost on the table's gray-900-ish surface. Lift the hue
  // to emerald-300 / 400 and rose-300 / 400, and add a faint same-hue halo
  // so each cell glows subtly without becoming a heatmap block.
  if (norm < 0.1) return "bg-emerald-500/20 font-bold text-emerald-300";
  if (norm < 0.2) return "bg-emerald-500/10 font-semibold text-emerald-400";
  if (norm < 0.35) return "";
  if (norm < 0.65) return "bg-rose-500/10 font-semibold text-rose-400";
  return "bg-rose-500/20 font-bold text-rose-300";
}
