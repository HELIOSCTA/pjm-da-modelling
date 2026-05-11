function fmtCorr(v: number | null): string {
  if (v == null) return "—";
  return v.toFixed(3);
}

function fmtPct(v: number | null): string {
  if (v == null) return "—";
  return `${v.toFixed(1)}%`;
}

export interface FitRow {
  series: string;
  color?: string;
  correlation: number | null;
  shape_mape: number | null;
}

// "How close is each forecast's shape to actual?" — Pearson r and shape
// MAPE per model series, side by side, with the best value per column
// shaded. Generalizes to N rows; needs actuals (empty otherwise).
export function ShapeFitTable({ rows }: { rows: FitRow[] }) {
  if (rows.length === 0) {
    return (
      <p className="text-xs text-gray-500">
        Shape fit metrics need actuals — they appear once DA LMP clears.
      </p>
    );
  }
  // Best correlation = highest; best MAPE = lowest. Highlight per column.
  const corrValues = rows.map((r) => r.correlation).filter((v): v is number => v != null);
  const mapeValues = rows.map((r) => r.shape_mape).filter((v): v is number => v != null);
  const bestCorr = corrValues.length ? Math.max(...corrValues) : null;
  const bestMape = mapeValues.length ? Math.min(...mapeValues) : null;

  const cellCls = (isBest: boolean): string =>
    "px-3 py-1.5 text-right tabular-nums " +
    (isBest ? "font-semibold text-emerald-300" : "text-gray-200");

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="border-b border-gray-800 text-gray-400">
            <th className="px-3 py-1.5 text-left font-medium">Series</th>
            <th className="px-3 py-1.5 text-right font-medium">Pearson r vs Actual</th>
            <th className="px-3 py-1.5 text-right font-medium">Shape MAPE vs Actual</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr
              key={r.series}
              className="border-b border-gray-900/60 hover:bg-gray-900/30"
            >
              <td className="px-3 py-1.5 font-medium text-gray-200">
                <span className="inline-flex items-center gap-2">
                  {r.color ? (
                    <span
                      className="inline-block h-2 w-2 rounded-full"
                      style={{ backgroundColor: r.color }}
                    />
                  ) : null}
                  {r.series}
                </span>
              </td>
              <td className={cellCls(r.correlation != null && r.correlation === bestCorr)}>
                {fmtCorr(r.correlation)}
              </td>
              <td className={cellCls(r.shape_mape != null && r.shape_mape === bestMape)}>
                {fmtPct(r.shape_mape)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
