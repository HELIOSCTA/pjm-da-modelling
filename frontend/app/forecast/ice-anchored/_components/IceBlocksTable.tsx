import type { BlockName, IceBlockEntry, IceSeriesLabel } from "@/types/forecast";

const SERIES_ORDER: readonly IceSeriesLabel[] = [
  "Det",
  "ENS Avg",
  "ENS Bottom",
  "ENS Top",
] as const;

const BLOCKS: readonly BlockName[] = ["OnPeak", "OffPeak", "Flat"] as const;

function fmt(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return v.toFixed(2);
}

// 4 series x 3 blocks pivot. Reads the flat block list from the payload.
export function IceBlocksTable({ blocks }: { blocks: IceBlockEntry[] }) {
  const lookup = new Map<string, number | null>();
  for (const b of blocks) lookup.set(`${b.series}|${b.block}`, b.value);

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="border-b border-gray-800 text-gray-400">
            <th className="px-3 py-1.5 text-left font-medium">Series</th>
            {BLOCKS.map((b) => (
              <th key={b} className="px-3 py-1.5 text-right font-medium">
                {b}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {SERIES_ORDER.map((s) => (
            <tr
              key={s}
              className="border-b border-gray-900/60 text-gray-200 hover:bg-gray-900/30"
            >
              <td className="px-3 py-1.5 font-medium">{s}</td>
              {BLOCKS.map((b) => (
                <td
                  key={b}
                  className="px-3 py-1.5 text-right tabular-nums"
                >
                  {fmt(lookup.get(`${s}|${b}`))}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
