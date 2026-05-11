import {
  HE_LIST,
  absErrorAtHE,
  aggregateByBlock,
  blockRMSE,
  errorAtHE,
  getHourlyByHE,
  mapeAtHE,
} from "@/lib/forecastMetrics";
import type { BlockName, HourlyForecastEntry } from "@/types/forecast";

import { HourlyMetricTable, type MetricRow } from "./HourlyMetricTable";

function fmt1(n: number | null): string | null {
  return n === null ? null : n.toFixed(1);
}
function fmt2(n: number | null): string | null {
  return n === null ? null : n.toFixed(2);
}
function signedFmt1(n: number | null): string | null {
  if (n === null) return null;
  const abs = Math.abs(n).toFixed(1);
  return n > 0 ? `+${abs}` : n < 0 ? `-${abs}` : abs;
}
function signedFmt2(n: number | null): string | null {
  if (n === null) return null;
  const abs = Math.abs(n).toFixed(2);
  return n > 0 ? `+${abs}` : n < 0 ? `-${abs}` : abs;
}
function pct1(n: number | null): string | null {
  return n === null ? null : `${n.toFixed(1)}%`;
}

function blockMap(
  agg: Record<BlockName, number | null>,
  fn: (v: number | null) => string | null,
): Record<BlockName, string | null> {
  return { OnPeak: fn(agg.OnPeak), OffPeak: fn(agg.OffPeak), Flat: fn(agg.Flat) };
}

export function ForecastVsActualsTable({
  hourly,
}: {
  hourly: HourlyForecastEntry[];
}) {
  const map = getHourlyByHE(hourly);
  const get = (he: number) => map.get(he);

  const actualPerHE = HE_LIST.map((he) => get(he)?.actual_lmp ?? null);
  const forecastPerHE = HE_LIST.map((he) => get(he)?.point_forecast ?? null);
  const errorPerHE = HE_LIST.map((he) => {
    const r = get(he);
    return r ? errorAtHE(r) : null;
  });
  const absErrorPerHE = HE_LIST.map((he) => {
    const r = get(he);
    return r ? absErrorAtHE(r) : null;
  });
  const mapePerHE = HE_LIST.map((he) => {
    const r = get(he);
    return r ? mapeAtHE(r) : null;
  });

  const errorAgg = aggregateByBlock(errorPerHE);
  const absErrorAgg = aggregateByBlock(absErrorPerHE);
  const mapeAgg = aggregateByBlock(mapePerHE);

  const rows: MetricRow[] = [
    {
      label: "Actual",
      className: "font-bold text-purple-400",
      perHE: actualPerHE.map(fmt1),
      perBlock: blockMap(aggregateByBlock(actualPerHE), fmt2),
    },
    {
      label: "Forecast",
      className: "font-bold text-sky-300",
      perHE: forecastPerHE.map(fmt1),
      perBlock: blockMap(aggregateByBlock(forecastPerHE), fmt2),
      dividerAfter: true, // separates outputs (Actual / Forecast) from error metrics
    },
    {
      label: "Error",
      className: "text-gray-300",
      perHE: errorPerHE.map(signedFmt1),
      perBlock: blockMap(errorAgg, signedFmt2),
      gradient: {
        kind: "abs_low_is_good",
        rawHE: errorPerHE,
        rawBlock: errorAgg,
      },
    },
    {
      label: "|Err|",
      className: "text-gray-300",
      perHE: absErrorPerHE.map(fmt1),
      perBlock: blockMap(absErrorAgg, fmt2),
      gradient: {
        kind: "low_is_good",
        rawHE: absErrorPerHE,
        rawBlock: absErrorAgg,
      },
    },
    {
      label: "MAPE %",
      className: "text-gray-300",
      perHE: mapePerHE.map(pct1),
      perBlock: blockMap(mapeAgg, pct1),
      gradient: { kind: "low_is_good", rawHE: mapePerHE, rawBlock: mapeAgg },
    },
  ];

  const rmseOnPeak = blockRMSE(hourly, "OnPeak");
  const rmseOffPeak = blockRMSE(hourly, "OffPeak");
  const rmseFlat = blockRMSE(hourly, "Flat");

  const footer = (
    <div className="font-mono">
      <span className="font-semibold text-amber-200">RMSE</span>
      {":  "}
      <span className="text-amber-200">OnPeak={fmt2(rmseOnPeak) ?? "—"}</span>
      {"   "}OffPeak={fmt2(rmseOffPeak) ?? "—"}
      {"   "}Flat={fmt2(rmseFlat) ?? "—"}
    </div>
  );

  return <HourlyMetricTable rows={rows} footer={footer} />;
}
