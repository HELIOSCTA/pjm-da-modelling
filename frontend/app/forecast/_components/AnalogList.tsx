import type { AnalogEntry } from "@/types/forecast";

function fmtPct(value: number): string {
  return (value * 100).toFixed(1) + "%";
}

function fmtMoney(value: number | null): string {
  return value === null ? "—" : `$${value.toFixed(2)}`;
}

export function AnalogList({ analogs }: { analogs: AnalogEntry[] }) {
  if (analogs.length === 0) {
    return <p className="text-sm text-gray-400">No analogs published.</p>;
  }
  return (
    <div className="overflow-x-auto rounded border border-gray-800">
      <table className="w-full border-collapse text-sm tabular-nums">
        <thead>
          <tr className="bg-[#161a23] text-gray-400">
            <th className="px-3 py-2 text-right font-medium">Rank</th>
            <th className="px-3 py-2 text-left font-medium">Analog Date</th>
            <th className="px-3 py-2 text-left font-medium">DOW</th>
            <th className="px-3 py-2 text-right font-medium">Δ Days</th>
            <th className="px-3 py-2 text-right font-medium">Weight</th>
            <th className="px-3 py-2 text-right font-medium">HEs / 24</th>
            <th className="px-3 py-2 text-right font-medium">DA OnPk LMP</th>
          </tr>
        </thead>
        <tbody>
          {analogs.map((a) => (
            <tr
              key={`${a.rank}-${a.analog_date}`}
              className="border-t border-gray-800 text-gray-200 hover:bg-[#161a23]"
            >
              <td className="px-3 py-1.5 text-right text-gray-400">{a.rank}</td>
              <td className="px-3 py-1.5">{a.analog_date}</td>
              <td className="px-3 py-1.5 text-gray-400">{a.day_of_week}</td>
              <td className="px-3 py-1.5 text-right text-gray-400">{a.day_diff}d</td>
              <td className="px-3 py-1.5 text-right">{fmtPct(a.weight_share)}</td>
              <td className="px-3 py-1.5 text-right text-gray-400">
                {a.hes_contributed}
              </td>
              <td className="px-3 py-1.5 text-right">{fmtMoney(a.da_onpk_lmp)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
