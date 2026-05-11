import {
  HE_LIST,
  aggregateByBlock,
  deltaP50AtHE,
  getHourlyByHE,
  iqrAtHE,
  skewAtHE,
  widthAtHE,
} from "@/lib/forecastMetrics";
import type {
  BlockBandEntry,
  BlockName,
  HourlyForecastEntry,
  QuantileLabel,
} from "@/types/forecast";

import { HourlyMetricTable, type MetricRow } from "./HourlyMetricTable";

const BAND_LABELS: QuantileLabel[] = [
  "P10",
  "P25",
  "P50",
  "Forecast",
  "P75",
  "P90",
];

const BAND_ROW_CLASS: Record<QuantileLabel, string> = {
  P10: "font-bold text-orange-500",
  P25: "text-orange-300",
  P50: "italic text-gray-300",
  Forecast: "font-bold text-sky-300",
  P75: "text-orange-300",
  P90: "font-bold text-orange-500",
};

function fmt1(n: number | null): string | null {
  return n === null ? null : n.toFixed(1);
}
function fmt2(n: number | null): string | null {
  return n === null ? null : n.toFixed(2);
}
function signedFmt2(n: number | null): string | null {
  if (n === null) return null;
  const abs = Math.abs(n).toFixed(2);
  return n > 0 ? `+${abs}` : n < 0 ? `-${abs}` : abs;
}

function quantileFromHourly(
  h: HourlyForecastEntry | undefined,
  label: QuantileLabel,
): number | null {
  if (!h) return null;
  switch (label) {
    case "P10":
      return h.q10;
    case "P25":
      return h.q25;
    case "P50":
      return h.q50;
    case "Forecast":
      return h.point_forecast;
    case "P75":
      return h.q75;
    case "P90":
      return h.q90;
  }
}

function blockValue(
  blocks: BlockBandEntry[],
  block: BlockName,
  label: QuantileLabel,
): number | null {
  return (
    blocks.find((b) => b.block === block && b.quantile_label === label)?.value ??
    null
  );
}

function blockMap(
  agg: Record<BlockName, number | null>,
  fn: (v: number | null) => string | null,
): Record<BlockName, string | null> {
  return { OnPeak: fn(agg.OnPeak), OffPeak: fn(agg.OffPeak), Flat: fn(agg.Flat) };
}

export function QuantileBandsTable({
  hourly,
  blocks,
}: {
  hourly: HourlyForecastEntry[];
  blocks: BlockBandEntry[];
}) {
  const map = getHourlyByHE(hourly);

  const bandRows: MetricRow[] = BAND_LABELS.map((label, idx) => ({
    label,
    className: BAND_ROW_CLASS[label],
    perHE: HE_LIST.map((he) => fmt1(quantileFromHourly(map.get(he), label))),
    perBlock: {
      OnPeak: fmt2(blockValue(blocks, "OnPeak", label)),
      OffPeak: fmt2(blockValue(blocks, "OffPeak", label)),
      Flat: fmt2(blockValue(blocks, "Flat", label)),
    },
    // Last band row separates the output bands from the diagnostic metrics.
    dividerAfter: idx === BAND_LABELS.length - 1,
  }));

  const widthByHE = HE_LIST.map((he) => {
    const h = map.get(he);
    return h ? widthAtHE(h) : null;
  });
  const iqrByHE = HE_LIST.map((he) => {
    const h = map.get(he);
    return h ? iqrAtHE(h) : null;
  });
  const skewByHE = HE_LIST.map((he) => {
    const h = map.get(he);
    return h ? skewAtHE(h) : null;
  });
  const dP50ByHE = HE_LIST.map((he) => deltaP50AtHE(hourly, he));

  const widthAgg = aggregateByBlock(widthByHE);
  const iqrAgg = aggregateByBlock(iqrByHE);
  const skewAgg = aggregateByBlock(skewByHE);
  const dP50Agg = aggregateByBlock(dP50ByHE);

  const diagnosticRows: MetricRow[] = [
    {
      label: "Width",
      className: "text-gray-400",
      perHE: widthByHE.map(fmt2),
      perBlock: blockMap(widthAgg, fmt2),
      gradient: { kind: "low_is_good", rawHE: widthByHE, rawBlock: widthAgg },
    },
    {
      label: "IQR",
      className: "text-gray-400",
      perHE: iqrByHE.map(fmt2),
      perBlock: blockMap(iqrAgg, fmt2),
      gradient: { kind: "low_is_good", rawHE: iqrByHE, rawBlock: iqrAgg },
    },
    {
      label: "Skew",
      className: "text-gray-400",
      perHE: skewByHE.map(signedFmt2),
      perBlock: blockMap(skewAgg, signedFmt2),
      gradient: { kind: "abs_low_is_good", rawHE: skewByHE, rawBlock: skewAgg },
    },
    {
      label: "Δ P50",
      className: "text-gray-400",
      perHE: dP50ByHE.map(signedFmt2),
      perBlock: blockMap(dP50Agg, signedFmt2),
      gradient: { kind: "abs_low_is_good", rawHE: dP50ByHE, rawBlock: dP50Agg },
    },
  ];

  const caption = (
    <span>
      Per-HE bands: weighted quantile of analog LMPs at that hour. OnPeak /
      OffPeak / Flat aggregates: per-date joint sampling — within-day price
      comovement preserved. Diagnostic rows shaded dark green = good (tight
      band / near-zero shape) → dark red = bad.
    </span>
  );

  return (
    <HourlyMetricTable rows={[...bandRows, ...diagnosticRows]} caption={caption} />
  );
}
