import { aggregateByBlock, meanOrNull, rmseOrNull } from "@/lib/forecastMetrics";

export interface LevelTableModel {
  key: string;
  label: string;
  color: string;
  /** dense 24-length point forecast, index = HE - 1 */
  forecast: Array<number | null>;
  /** forecast_runs.da_lmp_total_onpeak_forecast for the rendered run */
  onpeakPublished: number | null;
}

function fmt2(v: number | null): string {
  return v == null || !Number.isFinite(v) ? "—" : v.toFixed(2);
}

function signed2(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return "—";
  const a = Math.abs(v).toFixed(2);
  return v > 0 ? `+${a}` : v < 0 ? `-${a}` : a;
}

function errorSeries(
  forecast: Array<number | null>,
  actual: Array<number | null>,
): Array<number | null> {
  return forecast.map((f, i) => {
    const a = actual[i];
    return f != null && a != null && Number.isFinite(f) && Number.isFinite(a)
      ? f - a
      : null;
  });
}

// Per-model block summary ($/MWh): OnPeak / OffPeak / Flat means recomputed
// from the hourly point forecast, the published on-peak headline, and —
// once DA LMP clears — ME / MAE / RMSE vs Actual DA across all 24 HE.
export function LevelTable({
  models,
  actual,
}: {
  models: LevelTableModel[];
  actual: Array<number | null> | null;
}) {
  const hasActual = actual != null && actual.some((v) => v != null);
  const errCols = hasActual ? (["ME", "MAE", "RMSE"] as const) : ([] as const);

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="border-b border-gray-800 text-gray-400">
            <th className="px-3 py-1.5 text-left font-medium">Series</th>
            <th className="px-3 py-1.5 text-right font-medium">OnPeak</th>
            <th className="px-3 py-1.5 text-right font-medium">OffPeak</th>
            <th className="px-3 py-1.5 text-right font-medium">Flat</th>
            <th className="px-3 py-1.5 text-right font-medium">OnPk fcst*</th>
            {errCols.map((c) => (
              <th key={c} className="px-3 py-1.5 text-right font-medium">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {models.map((m) => {
            const blk = aggregateByBlock(m.forecast);
            const errs =
              hasActual && actual ? errorSeries(m.forecast, actual) : null;
            const me = errs ? meanOrNull(errs) : null;
            const mae = errs
              ? meanOrNull(errs.map((e) => (e == null ? null : Math.abs(e))))
              : null;
            const rmse = errs ? rmseOrNull(errs) : null;
            return (
              <tr
                key={m.key}
                className="border-b border-gray-900/60 hover:bg-gray-900/30"
              >
                <td
                  className="px-3 py-1.5 font-medium"
                  style={{ color: m.color }}
                >
                  {m.label}
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums text-gray-200">
                  {fmt2(blk.OnPeak)}
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums text-gray-200">
                  {fmt2(blk.OffPeak)}
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums text-gray-200">
                  {fmt2(blk.Flat)}
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums text-gray-400">
                  {fmt2(m.onpeakPublished)}
                </td>
                {hasActual ? (
                  <>
                    <td className="px-3 py-1.5 text-right tabular-nums text-gray-300">
                      {signed2(me)}
                    </td>
                    <td className="px-3 py-1.5 text-right tabular-nums text-gray-300">
                      {fmt2(mae)}
                    </td>
                    <td className="px-3 py-1.5 text-right tabular-nums text-gray-300">
                      {fmt2(rmse)}
                    </td>
                  </>
                ) : null}
              </tr>
            );
          })}
          {hasActual && actual ? (
            <tr className="border-t border-gray-800 bg-gray-900/20">
              <td className="px-3 py-1.5 font-bold text-purple-400">Actual DA</td>
              {(() => {
                const blk = aggregateByBlock(actual);
                return (
                  <>
                    <td className="px-3 py-1.5 text-right tabular-nums text-purple-300">
                      {fmt2(blk.OnPeak)}
                    </td>
                    <td className="px-3 py-1.5 text-right tabular-nums text-purple-300">
                      {fmt2(blk.OffPeak)}
                    </td>
                    <td className="px-3 py-1.5 text-right tabular-nums text-purple-300">
                      {fmt2(blk.Flat)}
                    </td>
                  </>
                );
              })()}
              <td className="px-3 py-1.5 text-right text-gray-600">—</td>
              <td className="px-3 py-1.5 text-right text-gray-600">—</td>
              <td className="px-3 py-1.5 text-right text-gray-600">—</td>
              <td className="px-3 py-1.5 text-right text-gray-600">—</td>
            </tr>
          ) : null}
        </tbody>
      </table>
      <p className="mt-2 text-[11px] leading-relaxed text-gray-500">
        OnPeak = HE8–23 mean · OffPeak = the rest · Flat = all 24 HE. ME / MAE
        / RMSE in $/MWh vs Actual DA across all 24 HE.{" "}
        <span className="text-gray-600">
          * &quot;OnPk fcst&quot; = the on-peak point forecast as published to{" "}
          <code>forecast_runs.da_lmp_total_onpeak_forecast</code> (should track
          the recomputed OnPeak column).
        </span>
      </p>
    </div>
  );
}
