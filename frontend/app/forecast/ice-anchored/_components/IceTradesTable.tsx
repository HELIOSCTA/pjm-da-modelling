import type { IceTradeEntry } from "@/types/forecast";

const DIRECTION_STYLE: Record<string, string> = {
  Lift: "text-emerald-300",
  Hit: "text-rose-300",
  Leg: "text-gray-400",
};

function fmtPrice(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return `$${v.toFixed(2)}`;
}

function fmtQty(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return Math.round(v).toLocaleString();
}

function fmtTime(iso: string | null): string {
  if (!iso) return "—";
  // Show HH:MM only — date is in the page header.
  return iso.length >= 16 ? iso.slice(11, 16) : iso;
}

// Trades are sorted by exec_time_local ASC in the publisher; reverse
// here so the most recent fills are at the top (matches the trader-facing
// terminal output in pjm_da_next_day_ticker_feed.py).
export function IceTradesTable({ trades }: { trades: IceTradeEntry[] }) {
  if (trades.length === 0) {
    return <p className="text-xs text-gray-500">No trades on this run.</p>;
  }
  const reversed = [...trades].reverse();

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="border-b border-gray-800 text-gray-400">
            <th className="px-3 py-1.5 text-left font-medium">Time</th>
            <th className="px-3 py-1.5 text-left font-medium">Direction</th>
            <th className="px-3 py-1.5 text-right font-medium">Price</th>
            <th className="px-3 py-1.5 text-right font-medium">Qty</th>
          </tr>
        </thead>
        <tbody>
          {reversed.map((t, i) => {
            const dir = t.trade_direction ?? "?";
            const cls = DIRECTION_STYLE[dir] ?? "text-gray-300";
            return (
              <tr
                key={`${t.exec_time_local}-${i}`}
                className="border-b border-gray-900/60 text-gray-200 hover:bg-gray-900/30"
              >
                <td className="px-3 py-1.5 font-mono">{fmtTime(t.exec_time_local)}</td>
                <td className={`px-3 py-1.5 font-medium ${cls}`}>{dir}</td>
                <td className="px-3 py-1.5 text-right font-mono tabular-nums">
                  {fmtPrice(t.price)}
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums">
                  {fmtQty(t.quantity)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
