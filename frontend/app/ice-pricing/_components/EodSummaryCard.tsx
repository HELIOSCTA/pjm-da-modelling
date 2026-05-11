import { describeSymbol } from "@/lib/iceSymbols";
import type { IceEodRow } from "@/types/icePricing";

function fmtNum(v: number | null, digits = 2): string {
  if (v === null || v === undefined) return "—";
  return Number(v).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function fmtInt(v: number | null): string {
  if (v === null || v === undefined) return "—";
  return Number(v).toLocaleString();
}

function Pair({
  label,
  value,
  className = "text-gray-200",
}: {
  label: string;
  value: string;
  className?: string;
}) {
  return (
    <span className="whitespace-nowrap">
      <span className="text-[10px] uppercase tracking-wider text-gray-500">
        {label}
      </span>{" "}
      <span className={`font-mono ${className}`}>{value}</span>
    </span>
  );
}

function Lane({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-baseline gap-x-5 gap-y-1.5 border-t border-gray-800 px-4 py-2 text-sm">
      <span className="w-16 shrink-0 text-[10px] uppercase tracking-wider text-gray-500">
        {label}
      </span>
      {children}
    </div>
  );
}

export function EodSummaryCard({ row }: { row: IceEodRow }) {
  const description = row.description ?? describeSymbol(row.symbol);
  const showDescription = description && description !== row.symbol;
  const change =
    row.close !== null && row.open !== null ? row.close - row.open : null;
  const changeClass =
    change === null
      ? "text-gray-300"
      : change > 0
        ? "text-emerald-300"
        : change < 0
          ? "text-rose-300"
          : "text-gray-300";
  const changeArrow = change === null ? "" : change > 0 ? " ↑" : change < 0 ? " ↓" : "";

  return (
    <section className="rounded border border-gray-800 bg-[#161a23]">
      <header className="flex flex-wrap items-baseline justify-between gap-x-6 gap-y-1 px-4 py-2.5">
        <div className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5">
          <span className="font-mono text-sm font-semibold text-gray-100">
            {row.symbol}
          </span>
          {showDescription ? (
            <span className="text-xs text-gray-400">{description}</span>
          ) : null}
          <span className="text-[11px] text-gray-500">
            {[row.product_type, row.contract_type, row.strip]
              .filter(Boolean)
              .join(" · ")}
            {row.start_date && row.end_date ? (
              <>
                {" · "}delivery {row.start_date}
                {row.start_date !== row.end_date ? ` → ${row.end_date}` : ""}
              </>
            ) : null}
          </span>
        </div>
        <span className="text-[11px] text-gray-500">
          trade_date {row.trade_date}
        </span>
      </header>

      <Lane label="Price">
        <Pair label="Open" value={fmtNum(row.open)} />
        <Pair label="High" value={fmtNum(row.high)} />
        <Pair label="Low" value={fmtNum(row.low)} />
        <Pair
          label="Close"
          value={fmtNum(row.close)}
          className="font-semibold text-gray-50"
        />
        <Pair
          label="VWAP"
          value={fmtNum(row.vwap)}
          className="font-semibold text-gray-50"
        />
        <Pair
          label="Δ"
          value={`${change === null ? "—" : (change > 0 ? "+" : "") + fmtNum(change)}${changeArrow}`}
          className={changeClass}
        />
      </Lane>

      <Lane label="Activity">
        <Pair label="Vol" value={fmtInt(row.volume)} />
        <Pair label="Trades" value={fmtInt(row.trade_count)} />
        <Pair label="Lift" value={fmtInt(row.lift_count)} />
        <Pair label="Hit" value={fmtInt(row.hit_count)} />
        <Pair label="Leg" value={fmtInt(row.leg_count)} />
      </Lane>

      <Lane label="Other">
        <Pair
          label="Block"
          value={`${fmtInt(row.block_trade_count)} / ${fmtInt(row.block_volume)}`}
        />
        <Pair label="Buy vol" value={fmtInt(row.buy_volume)} />
        <Pair label="Sell vol" value={fmtInt(row.sell_volume)} />
        <Pair label="Leg vol" value={fmtInt(row.leg_volume)} />
      </Lane>
    </section>
  );
}
