import {
  listAvailableTargetDates,
  listForecastRuns,
  readForecastRun,
  readLatestForecastRun,
} from "@/lib/server/forecastRuns";
import { isIcePayload } from "@/types/forecast";

import { ActualsStatusBanner } from "../_components/ActualsStatusBanner";
import { CollapsibleCard } from "../_components/CollapsibleCard";
import { DatePicker } from "../_components/DatePicker";
import { EmptyStatePanel } from "../_components/EmptyStatePanel";
import { PageHeader, RunMetadata } from "../_components/PageHeader";
import { RunPicker } from "../_components/RunPicker";
import { SubCard } from "../_components/SubCard";

import { IceAnchorCard } from "./_components/IceAnchorCard";
import { IceBlocksTable } from "./_components/IceBlocksTable";
import { IceForecastChart } from "./_components/IceForecastChart";
import { IceHourlyTable } from "./_components/IceHourlyTable";
import { IceTradesTable } from "./_components/IceTradesTable";

const MODEL_NAME = "baseline_meteo_da_price_ice_anchored";

function defaultTargetDate(): string {
  const t = new Date();
  t.setUTCDate(t.getUTCDate() + 1);
  return t.toISOString().slice(0, 10);
}

export default async function IceAnchoredPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const rawDate = params.target_date;
  const rawRunId = params.run_id;
  const targetDate =
    typeof rawDate === "string" && rawDate.length > 0 ? rawDate : defaultTargetDate();
  const runId =
    typeof rawRunId === "string" && rawRunId.length > 0 ? rawRunId : null;

  const [rawPayload, runs, availableDates] = await Promise.all([
    runId
      ? readForecastRun(MODEL_NAME, targetDate, runId)
      : readLatestForecastRun(MODEL_NAME, targetDate),
    listForecastRuns(MODEL_NAME, targetDate),
    listAvailableTargetDates(MODEL_NAME),
  ]);

  if (!rawPayload) {
    return (
      <main className="px-8 py-8">
        <PageHeader title="ICE-Anchored Meteo Forecast" />
        <div className="mb-6 flex flex-wrap items-center gap-4">
          <DatePicker dates={availableDates} activeDate={targetDate} />
        </div>
        <EmptyStatePanel
          message="No forecast run found for this date."
          hint={
            <>
              Run the publisher with <code>METEO_ICE_PUBLISH=1</code> for{" "}
              <code>{targetDate}</code>, then refresh.
            </>
          }
        />
      </main>
    );
  }

  if (!isIcePayload(rawPayload)) {
    return (
      <main className="px-8 py-8">
        <PageHeader title="ICE-Anchored Meteo Forecast" />
        <EmptyStatePanel
          message={
            <>
              Unexpected model_family <code>{rawPayload.model_family}</code> for{" "}
              <code>{MODEL_NAME}</code>.
            </>
          }
        />
      </main>
    );
  }
  const payload = rawPayload;

  const actualsReleased = payload.hourly.some((h) => h.actual_lmp != null);

  return (
    <main className="px-8 py-8">
      <PageHeader
        title={<>ICE-Anchored Meteo — {payload.hub}</>}
        subline={
          <>
            model{" "}
            <span className="font-mono text-gray-300">{payload.model_name}</span>
            {payload.lead_days != null
              ? ` · lead_days=${payload.lead_days}`
              : null}
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
        <IceAnchorCard anchor={payload.ice_anchor} />
      </div>

      <div className="mb-6">
        <CollapsibleCard title="Hourly Forecast">
          <IceForecastChart hourly={payload.hourly} />
          <div className="mt-6 space-y-3">
            <SubCard title="Hourly values ($/MWh)">
              <IceHourlyTable hourly={payload.hourly} />
            </SubCard>
            <SubCard title="Block bands ($/MWh)">
              <IceBlocksTable blocks={payload.blocks} />
            </SubCard>
          </div>
        </CollapsibleCard>
      </div>

      <CollapsibleCard
        title="ICE Trades"
        badge={`${payload.ice_trades.length} trades`}
      >
        <IceTradesTable trades={payload.ice_trades} />
      </CollapsibleCard>
    </main>
  );
}
