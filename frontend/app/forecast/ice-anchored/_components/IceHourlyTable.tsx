import type { IceHourlyEntry } from "@/types/forecast";

const ROWS = [
  { key: "actual_lmp", label: "Actual" },
  { key: "point_forecast", label: "Det" },
  { key: "ens_avg", label: "ENS Avg" },
  { key: "ens_bottom", label: "ENS Bot" },
  { key: "ens_top", label: "ENS Top" },
] as const;

function fmt(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return v.toFixed(1);
}

function err(actual: number | null, forecast: number | null): number | null {
  if (actual == null || forecast == null) return null;
  return forecast - actual;
}

// Server-rendered table — HE1..HE24 across, one row per series.
// Includes a derived Error row (Det - Actual) when actuals are present.
export function IceHourlyTable({ hourly }: { hourly: IceHourlyEntry[] }) {
  const byHe = new Map(hourly.map((h) => [h.hour_ending, h]));
  const hasActuals = hourly.some((h) => h.actual_lmp != null);

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="border-b border-gray-800 text-gray-400">
            <th className="sticky left-0 bg-gray-900/40 px-2 py-1.5 text-left font-medium">
              Series
            </th>
            {Array.from({ length: 24 }, (_, i) => i + 1).map((he) => (
              <th
                key={he}
                className="px-1.5 py-1.5 text-right font-medium tabular-nums"
              >
                HE{he}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {ROWS.map((row) => {
            if (row.key === "actual_lmp" && !hasActuals) return null;
            return (
              <tr
                key={row.key}
                className="border-b border-gray-900/60 text-gray-200 hover:bg-gray-900/30"
              >
                <td className="sticky left-0 bg-gray-900/40 px-2 py-1.5 font-medium">
                  {row.label}
                </td>
                {Array.from({ length: 24 }, (_, i) => i + 1).map((he) => {
                  const v = byHe.get(he)?.[row.key] ?? null;
                  return (
                    <td
                      key={he}
                      className="px-1.5 py-1.5 text-right tabular-nums"
                    >
                      {fmt(v)}
                    </td>
                  );
                })}
              </tr>
            );
          })}
          {hasActuals ? (
            <tr className="border-b border-gray-900/60 text-amber-300/90">
              <td className="sticky left-0 bg-gray-900/40 px-2 py-1.5 font-medium">
                Err (Det)
              </td>
              {Array.from({ length: 24 }, (_, i) => i + 1).map((he) => {
                const h = byHe.get(he);
                const e = err(h?.actual_lmp ?? null, h?.point_forecast ?? null);
                return (
                  <td
                    key={he}
                    className="px-1.5 py-1.5 text-right tabular-nums"
                  >
                    {e == null ? "—" : (e >= 0 ? `+${e.toFixed(1)}` : e.toFixed(1))}
                  </td>
                );
              })}
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}
