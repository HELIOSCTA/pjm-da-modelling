import {
  HE_LIST,
  aggregateByBlock,
  blockInBand80Coverage,
  crpsAtHE,
  getHourlyByHE,
  inBand80AtHE,
} from "@/lib/forecastMetrics";
import type {
  BlockBandEntry,
  BlockName,
  HourlyForecastEntry,
} from "@/types/forecast";

import { HourlyMetricTable, type MetricRow } from "./HourlyMetricTable";

function fmt1(n: number | null): string | null {
  return n === null ? null : n.toFixed(1);
}
function fmt2(n: number | null): string | null {
  return n === null ? null : n.toFixed(2);
}
function fmt3(n: number | null): string | null {
  return n === null ? null : n.toFixed(3);
}

function blockValue(
  blocks: BlockBandEntry[],
  block: BlockName,
  label: "P10" | "P90",
): number | null {
  return (
    blocks.find((b) => b.block === block && b.quantile_label === label)?.value ??
    null
  );
}

export function BandsVsActualsTable({
  hourly,
  blocks,
}: {
  hourly: HourlyForecastEntry[];
  blocks: BlockBandEntry[];
}) {
  const map = getHourlyByHE(hourly);
  const get = (he: number) => map.get(he);

  const p10ByHE = HE_LIST.map((he) => get(he)?.q10 ?? null);
  const actualByHE = HE_LIST.map((he) => get(he)?.actual_lmp ?? null);
  const p90ByHE = HE_LIST.map((he) => get(he)?.q90 ?? null);
  const inBandByHE = HE_LIST.map((he) => {
    const r = get(he);
    return r ? inBand80AtHE(r) : null;
  });
  const crpsByHE = HE_LIST.map((he) => {
    const r = get(he);
    return r ? crpsAtHE(r) : null;
  });

  const inBandPct = (
    cov: { covered: number; total: number } | null,
  ): string | null =>
    cov === null || cov.total === 0
      ? null
      : `${Math.round((100 * cov.covered) / cov.total)}%`;

  const inBandPctNum = (
    cov: { covered: number; total: number } | null,
  ): number | null =>
    cov === null || cov.total === 0 ? null : (100 * cov.covered) / cov.total;

  const crpsAgg = aggregateByBlock(crpsByHE);

  const rows: MetricRow[] = [
    {
      label: "P10",
      className: "font-bold text-orange-500",
      perHE: p10ByHE.map(fmt1),
      perBlock: {
        OnPeak: fmt2(blockValue(blocks, "OnPeak", "P10")),
        OffPeak: fmt2(blockValue(blocks, "OffPeak", "P10")),
        Flat: fmt2(blockValue(blocks, "Flat", "P10")),
      },
    },
    {
      label: "Actual",
      className: "font-bold text-purple-400",
      perHE: actualByHE.map(fmt1),
      perBlock: {
        OnPeak: fmt2(aggregateByBlock(actualByHE).OnPeak),
        OffPeak: fmt2(aggregateByBlock(actualByHE).OffPeak),
        Flat: fmt2(aggregateByBlock(actualByHE).Flat),
      },
    },
    {
      label: "P90",
      className: "font-bold text-orange-500",
      perHE: p90ByHE.map(fmt1),
      perBlock: {
        OnPeak: fmt2(blockValue(blocks, "OnPeak", "P90")),
        OffPeak: fmt2(blockValue(blocks, "OffPeak", "P90")),
        Flat: fmt2(blockValue(blocks, "Flat", "P90")),
      },
      dividerAfter: true, // separates band/actual outputs from coverage metrics
    },
    {
      label: "InBand 80%",
      className: "text-gray-300",
      perHE: inBandByHE.map((v) => (v === null ? null : v ? "✓" : "✗")),
      perHEClass: inBandByHE.map((v) =>
        v === null
          ? null
          : v
            ? "font-bold text-emerald-400"
            : "font-bold text-rose-400",
      ),
      perBlock: {
        OnPeak: inBandPct(blockInBand80Coverage(hourly, "OnPeak")),
        OffPeak: inBandPct(blockInBand80Coverage(hourly, "OffPeak")),
        Flat: inBandPct(blockInBand80Coverage(hourly, "Flat")),
      },
      perBlockClass: {
        OnPeak: gradeCoverageClass(
          inBandPctNum(blockInBand80Coverage(hourly, "OnPeak")),
        ),
        OffPeak: gradeCoverageClass(
          inBandPctNum(blockInBand80Coverage(hourly, "OffPeak")),
        ),
        Flat: gradeCoverageClass(
          inBandPctNum(blockInBand80Coverage(hourly, "Flat")),
        ),
      },
    },
    {
      label: "CRPS",
      className: "text-gray-400",
      perHE: crpsByHE.map(fmt3),
      perBlock: {
        OnPeak: fmt3(crpsAgg.OnPeak),
        OffPeak: fmt3(crpsAgg.OffPeak),
        Flat: fmt3(crpsAgg.Flat),
      },
      gradient: { kind: "low_is_good", rawHE: crpsByHE, rawBlock: crpsAgg },
    },
  ];

  return <HourlyMetricTable rows={rows} />;
}

// Coverage % classifier — 80% nominal coverage is the target, so >=80%
// reads as good and well below as bad.
function gradeCoverageClass(pct: number | null): string {
  if (pct === null) return "";
  if (pct >= 80) return "font-bold text-emerald-400";
  if (pct >= 65) return "text-emerald-300";
  if (pct >= 50) return "text-rose-300";
  return "font-bold text-rose-400";
}
