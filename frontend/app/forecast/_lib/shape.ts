// Shape = price profile with the level normalized out. We divide each
// HE value by the series' own 24-hour mean, so a value of 1.20 at HE19
// means "20% above the day's flat avg". This makes per-series shapes
// directly comparable regardless of absolute level (KNN at $40 average
// vs ICE-anchored at $42 average vs Actual at $43 average all become
// dimensionless ratios on the same axis).

export interface ShapePoint {
  hour_ending: number;
  ratio: number | null;
}

export function toShape(values: ReadonlyArray<number | null>): ShapePoint[] {
  const finite = values.filter((v): v is number => v != null && Number.isFinite(v));
  if (finite.length === 0) {
    return values.map((_, i) => ({ hour_ending: i + 1, ratio: null }));
  }
  const mean = finite.reduce((s, v) => s + v, 0) / finite.length;
  if (!Number.isFinite(mean) || mean === 0) {
    return values.map((_, i) => ({ hour_ending: i + 1, ratio: null }));
  }
  return values.map((v, i) => ({
    hour_ending: i + 1,
    ratio: v == null || !Number.isFinite(v) ? null : v / mean,
  }));
}

// Pearson correlation across hours where both series are finite. Used
// as a single-number "shape fit" between a forecast and the actual.
export function shapeCorrelation(
  a: ReadonlyArray<number | null>,
  b: ReadonlyArray<number | null>,
): number | null {
  const pairs: Array<[number, number]> = [];
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const va = a[i];
    const vb = b[i];
    if (va == null || vb == null || !Number.isFinite(va) || !Number.isFinite(vb)) {
      continue;
    }
    pairs.push([va, vb]);
  }
  if (pairs.length < 2) return null;
  const meanA = pairs.reduce((s, [x]) => s + x, 0) / pairs.length;
  const meanB = pairs.reduce((s, [, y]) => s + y, 0) / pairs.length;
  let num = 0,
    da = 0,
    db = 0;
  for (const [x, y] of pairs) {
    const dx = x - meanA;
    const dy = y - meanB;
    num += dx * dy;
    da += dx * dx;
    db += dy * dy;
  }
  if (da === 0 || db === 0) return null;
  return num / Math.sqrt(da * db);
}

// MAPE between two shape arrays (already-normalized ratios). Skips
// entries where either side is null.
export function shapeMape(
  forecast: ReadonlyArray<ShapePoint>,
  actual: ReadonlyArray<ShapePoint>,
): number | null {
  const aByHe = new Map(actual.map((p) => [p.hour_ending, p.ratio]));
  let sum = 0;
  let n = 0;
  for (const p of forecast) {
    const a = aByHe.get(p.hour_ending);
    if (p.ratio == null || a == null || a === 0) continue;
    sum += Math.abs((p.ratio - a) / a);
    n++;
  }
  return n === 0 ? null : (sum / n) * 100;
}
