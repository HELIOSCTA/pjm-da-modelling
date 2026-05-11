import { COMPARE_SERIES_COLORS } from "@/lib/chartConstants";
import {
  listAvailableTargetDates,
  listForecastRuns,
  readForecastRun,
  readLatestForecastRun,
} from "@/lib/server/forecastRuns";
import {
  isIcePayload,
  isKnnPayload,
  type ForecastRunPayload,
} from "@/types/forecast";

import { ActualsStatusBanner } from "../_components/ActualsStatusBanner";
import { CollapsibleCard } from "../_components/CollapsibleCard";
import { DatePicker } from "../_components/DatePicker";
import { EmptyStatePanel } from "../_components/EmptyStatePanel";
import { PageHeader, RunMetadata } from "../_components/PageHeader";
import { RunPicker } from "../_components/RunPicker";
import { SubCard } from "../_components/SubCard";
import { COMPARE_MODELS } from "../_lib/tabs";
import { shapeCorrelation, shapeMape, toShape } from "../_lib/shape";

import { LevelChart } from "./_components/LevelChart";
import { LevelTable } from "./_components/LevelTable";
import { ShapeChart } from "./_components/ShapeChart";
import { ShapeFitTable } from "./_components/ShapeFitTable";

function defaultTargetDate(): string {
  // Tomorrow in UTC — mirrors the Python publishers' default
  // (target_date = date.today() + 1). Off by a few hours around the ET
  // midnight boundary; fine for a landing default.
  const t = new Date();
  t.setUTCDate(t.getUTCDate() + 1);
  return t.toISOString().slice(0, 10);
}

function unionDates(lists: string[][]): string[] {
  return [...new Set(lists.flat())].sort().reverse();
}

// Both KNN and ICE payloads expose hourly[].hour_ending / point_forecast /
// actual_lmp — that's all the compare view reads. Build a dense 24-length
// array so missing HEs become null.
function denseHourly(
  payload: ForecastRunPayload,
  field: "point_forecast" | "actual_lmp",
): Array<number | null> {
  const hourly: Array<{
    hour_ending: number;
    point_forecast: number | null;
    actual_lmp: number | null;
  }> = isKnnPayload(payload)
    ? payload.hourly
    : isIcePayload(payload)
      ? payload.hourly
      : [];
  return Array.from({ length: 24 }, (_, i) => {
    const h = hourly.find((x) => x.hour_ending === i + 1);
    return h ? h[field] : null;
  });
}

function fmtMoney(v: number | null): string {
  return v == null || !Number.isFinite(v) ? "—" : `$${v.toFixed(2)}`;
}

export default async function ComparePage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const rawDate = params.target_date;
  const targetDate =
    typeof rawDate === "string" && rawDate.length > 0
      ? rawDate
      : defaultTargetDate();

  // Per-model run-picker selection (URL param `run_<key>`).
  const pickedRunId = new Map<string, string | null>();
  for (const m of COMPARE_MODELS) {
    const raw = params[`run_${m.key}`];
    pickedRunId.set(
      m.key,
      typeof raw === "string" && raw.length > 0 ? raw : null,
    );
  }

  // One fetch bundle per model, all in flight: the explicitly-picked run (if
  // any), the latest run (always — fallback when the picked id is stale), the
  // run list (for the picker + the published on-peak headline), and the
  // model's available target dates.
  const perModel = await Promise.all(
    COMPARE_MODELS.map(async (m, idx) => {
      const runId = pickedRunId.get(m.key) ?? null;
      const [picked, latest, runs, dates] = await Promise.all([
        runId
          ? readForecastRun(m.modelName, targetDate, runId)
          : Promise.resolve(null),
        readLatestForecastRun(m.modelName, targetDate),
        listForecastRuns(m.modelName, targetDate),
        listAvailableTargetDates(m.modelName),
      ]);
      const payload = picked ?? latest;
      const forecast = payload ? denseHourly(payload, "point_forecast") : null;
      const actual = payload ? denseHourly(payload, "actual_lmp") : null;
      const onpeakPublished = payload
        ? runs.find((r) => r.run_id === payload.run_id)
            ?.da_lmp_total_onpeak_forecast ?? null
        : null;
      return {
        key: m.key,
        label: m.label,
        color: COMPARE_SERIES_COLORS[idx % COMPARE_SERIES_COLORS.length],
        effectiveRunId: picked ? runId : null,
        payload,
        runs,
        dates,
        forecast,
        actual,
        onpeakPublished,
      };
    }),
  );

  const resolved = perModel.filter(
    (
      m,
    ): m is typeof m & {
      payload: ForecastRunPayload;
      forecast: Array<number | null>;
    } => m.payload != null && m.forecast != null,
  );
  const missing = perModel.filter((m) => m.payload == null);
  const availableDates = unionDates(perModel.map((m) => m.dates));

  // Actual DA: prefer the first resolved model that carries actuals — they
  // agree (same DA LMP, same date) regardless of which model wrote them.
  const actualValues =
    resolved.find((m) => m.actual?.some((v) => v != null))?.actual ?? null;
  const actualShape = actualValues ? toShape(actualValues) : null;
  const actualsReleased = actualValues != null;

  const hub = resolved[0]?.payload.hub ?? null;

  // Cross-model on-peak spread (the published headline number).
  const onpeakNums = resolved
    .map((m) => m.onpeakPublished)
    .filter((v): v is number => v != null && Number.isFinite(v));
  const spread =
    onpeakNums.length >= 2
      ? Math.max(...onpeakNums) - Math.min(...onpeakNums)
      : null;

  const levelSeries = resolved.map((m) => ({
    key: m.key,
    label: m.label,
    color: m.color,
    values: m.forecast,
  }));
  const shapeSeries = resolved.map((m) => ({
    key: m.key,
    label: m.label,
    color: m.color,
    points: toShape(m.forecast),
  }));
  const levelTableModels = resolved.map((m) => ({
    key: m.key,
    label: m.label,
    color: m.color,
    forecast: m.forecast,
    onpeakPublished: m.onpeakPublished,
  }));
  const fitRows = actualShape
    ? resolved.map((m) => ({
        series: m.label,
        color: m.color,
        correlation: shapeCorrelation(m.forecast, actualValues!),
        shape_mape: shapeMape(toShape(m.forecast), actualShape),
      }))
    : [];

  return (
    <main className="px-8 py-8">
      <PageHeader
        title={<>Compare models{hub ? <> — {hub}</> : null}</>}
        subline={
          <>
            Two reads of the same target date:{" "}
            <span className="font-semibold text-gray-300">Level</span> ($/MWh,
            absolute) and{" "}
            <span className="font-semibold text-gray-300">Shape</span> (each
            series ÷ its own 24-hour mean). Actual DA overlays once it clears.
          </>
        }
        rightMetadata={
          resolved.length > 0 ? (
            <div className="space-y-1">
              {resolved.map((m) => (
                <RunMetadata
                  key={m.key}
                  label={m.label}
                  createdAtUtc={m.payload.created_at_utc}
                  runId={m.payload.run_id}
                />
              ))}
            </div>
          ) : undefined
        }
      />

      <div className="mb-6 flex flex-wrap items-center gap-4">
        <DatePicker dates={availableDates} activeDate={targetDate} />
        {perModel.map((m) => (
          <RunPicker
            key={m.key}
            runs={m.runs}
            activeRunId={m.effectiveRunId}
            paramKey={`run_${m.key}`}
            label={`${m.label} run`}
          />
        ))}
      </div>

      {resolved.length > 0 ? (
        <ActualsStatusBanner released={actualsReleased} targetDate={targetDate} />
      ) : null}

      {missing.length > 0 && resolved.length > 0 ? (
        <p className="mb-4 text-xs text-amber-300/80">
          No run for {targetDate}: {missing.map((m) => m.label).join(", ")}.
        </p>
      ) : null}

      {resolved.length === 0 ? (
        <EmptyStatePanel
          message={<>No forecast runs found for {targetDate} on any model.</>}
          hint={
            <>
              Each model upserts a row into{" "}
              <code>pjm_model_outputs.forecast_runs</code> when its publisher
              runs. Pick a date with data above, or wait for the pipeline.
            </>
          }
        />
      ) : (
        <div className="space-y-6">
          <CollapsibleCard title="Level ($/MWh)">
            {spread != null ? (
              <p className="mb-4 text-xs text-gray-300">
                On-peak (HE8–23) point forecast:{" "}
                {resolved
                  .filter((m) => m.onpeakPublished != null)
                  .map((m) => `${m.label} ${fmtMoney(m.onpeakPublished)}`)
                  .join("  ·  ")}{"  ·  "}
                <span className="font-semibold text-amber-200">
                  spread {fmtMoney(spread)}
                </span>
              </p>
            ) : null}
            <LevelChart series={levelSeries} actual={actualValues} />
            <div className="mt-6">
              <SubCard title="Block summary ($/MWh)">
                <LevelTable models={levelTableModels} actual={actualValues} />
              </SubCard>
            </div>
          </CollapsibleCard>

          <CollapsibleCard title="Shape (HE1–HE24, ÷ own 24h mean)">
            <ShapeChart series={shapeSeries} actual={actualShape} />
            <p className="mt-2 text-xs text-gray-500">
              A value of 1.20 at HE19 means 20% above that series&apos; flat
              average — so profiles are comparable regardless of price level.
            </p>
            <div className="mt-6">
              <SubCard title="Shape fit vs Actual">
                <ShapeFitTable rows={fitRows} />
              </SubCard>
            </div>
          </CollapsibleCard>
        </div>
      )}
    </main>
  );
}
