import {
  listAvailableTargetDates,
  listForecastRuns,
  readForecastRun,
  readLatestForecastRun,
} from "@/lib/server/forecastRuns";
import { hasActuals } from "@/lib/forecastMetrics";
import { isKnnPayload } from "@/types/forecast";

import { ActualsStatusBanner } from "../_components/ActualsStatusBanner";
import { AnalogList } from "../_components/AnalogList";
import { BandsVsActualsTable } from "../_components/BandsVsActualsTable";
import { CollapsibleCard } from "../_components/CollapsibleCard";
import { DatePicker } from "../_components/DatePicker";
import { EmptyStatePanel } from "../_components/EmptyStatePanel";
import { ForecastChart } from "../_components/ForecastChart";
import { ForecastVsActualsTable } from "../_components/ForecastVsActualsTable";
import { PageHeader, RunMetadata } from "../_components/PageHeader";
import { QuantileBandsTable } from "../_components/QuantileBandsTable";
import { RunPicker } from "../_components/RunPicker";
import { SubCard } from "../_components/SubCard";

const MODEL_NAME = "pjm_rto_hourly";

function defaultTargetDate(): string {
  // Tomorrow in UTC. Mirrors the Python default
  // (target_date = date.today() + timedelta(days=1)). Off by a few hours
  // around the ET midnight boundary; acceptable for a default landing.
  const t = new Date();
  t.setUTCDate(t.getUTCDate() + 1);
  return t.toISOString().slice(0, 10);
}

export default async function ForecastPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const rawDate = params.target_date;
  const rawRunId = params.run_id;
  const targetDate =
    typeof rawDate === "string" && rawDate.length > 0 ? rawDate : defaultTargetDate();
  const modelName = MODEL_NAME;
  const runId =
    typeof rawRunId === "string" && rawRunId.length > 0 ? rawRunId : null;

  const [rawPayload, runs, availableDates] = await Promise.all([
    runId
      ? readForecastRun(modelName, targetDate, runId)
      : readLatestForecastRun(modelName, targetDate),
    listForecastRuns(modelName, targetDate),
    listAvailableTargetDates(modelName),
  ]);

  if (!rawPayload) {
    return (
      <main className="px-8 py-8">
        <PageHeader title="Like-Day KNN Forecast" />
        <div className="mb-6 flex flex-wrap items-center gap-4">
          <DatePicker dates={availableDates} activeDate={targetDate} />
        </div>
        <EmptyStatePanel
          message="No forecast run found for this date."
          hint={
            <>
              The Python pipeline upserts a row into{" "}
              <code>pjm_model_outputs.forecast_runs</code> on every run. Once it
              has run for <code>{targetDate}</code>, refresh this page.
            </>
          }
        />
      </main>
    );
  }

  // This page renders the KNN like-day shape. Other model_family values
  // (e.g. "baseline") have their own pages; treat as wrong-tab here.
  if (!isKnnPayload(rawPayload)) {
    return (
      <main className="px-8 py-8">
        <PageHeader title="Like-Day KNN Forecast" />
        <EmptyStatePanel
          message={
            <>
              model {modelName} is not a like-day model (got family{" "}
              <code>{rawPayload.model_family}</code>).
            </>
          }
        />
      </main>
    );
  }
  const payload = rawPayload;

  const actualsReleased = hasActuals(payload.hourly);

  return (
    <main className="px-8 py-8">
      <PageHeader
        title={<>Like-Day KNN — {payload.hub}</>}
        subline={
          <>
            {payload.day_type} · model{" "}
            <span className="font-mono text-gray-300">{payload.model_name}</span>
          </>
        }
        rightMetadata={
          <RunMetadata
            createdAtUtc={payload.created_at_utc}
            runId={payload.run_id}
          />
        }
      />

      <div className="mb-6 flex flex-wrap items-center gap-4">
        <DatePicker dates={availableDates} activeDate={targetDate} />
        <RunPicker runs={runs} activeRunId={runId} />
      </div>

      <ActualsStatusBanner
        released={actualsReleased}
        targetDate={payload.target_date}
      />

      <div className="mb-6">
        <CollapsibleCard title="Hourly Forecast">
          <ForecastChart hourly={payload.hourly} />
          <div className="mt-6 space-y-3">
            {actualsReleased ? (
              <SubCard title="Forecast vs Actuals">
                <ForecastVsActualsTable hourly={payload.hourly} />
              </SubCard>
            ) : null}
            <SubCard title="Quantile Bands ($/MWh)">
              <QuantileBandsTable
                hourly={payload.hourly}
                blocks={payload.blocks}
              />
            </SubCard>
            {actualsReleased ? (
              <SubCard title="Quantile Bands vs Actuals">
                <BandsVsActualsTable
                  hourly={payload.hourly}
                  blocks={payload.blocks}
                />
              </SubCard>
            ) : null}
          </div>
        </CollapsibleCard>
      </div>

      <CollapsibleCard
        title="Analog Days"
        badge={`${payload.n_unique_analog_dates} unique`}
      >
        <AnalogList analogs={payload.analogs} />
      </CollapsibleCard>
    </main>
  );
}
