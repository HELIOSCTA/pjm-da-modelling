import { NextResponse } from "next/server";

import {
  readForecastRun,
  readLatestForecastRun,
} from "@/lib/server/forecastRuns";

export const dynamic = "force-dynamic";

const DEFAULT_MODEL = "pjm_rto_hourly";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const targetDate = url.searchParams.get("target_date");
  const modelName = url.searchParams.get("model") ?? DEFAULT_MODEL;
  const runId = url.searchParams.get("run_id");

  if (!targetDate) {
    return NextResponse.json(
      { error: "target_date query param is required (YYYY-MM-DD)" },
      { status: 400 },
    );
  }

  const payload = runId
    ? await readForecastRun(modelName, targetDate, runId)
    : await readLatestForecastRun(modelName, targetDate);

  if (!payload) {
    return NextResponse.json(
      {
        error: runId
          ? `No forecast run for model=${modelName} target_date=${targetDate} run_id=${runId}`
          : `No forecast snapshot for model=${modelName} target_date=${targetDate}`,
      },
      { status: 404 },
    );
  }

  return NextResponse.json(payload);
}
