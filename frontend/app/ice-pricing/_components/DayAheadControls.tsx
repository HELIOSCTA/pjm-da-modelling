"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useMemo, useTransition } from "react";

import { groupSymbolsForPicker } from "@/lib/iceSymbols";

interface Props {
  tradeDate: string;
  symbol: string;
  symbols: string[];
}

export function DayAheadControls({ tradeDate, symbol, symbols }: Props) {
  const router = useRouter();
  const params = useSearchParams();
  const [pending, startTransition] = useTransition();

  function update(next: { trade_date?: string; symbol?: string }) {
    const sp = new URLSearchParams(params.toString());
    sp.set("tab", "day-ahead");
    if (next.trade_date !== undefined) sp.set("trade_date", next.trade_date);
    if (next.symbol !== undefined) sp.set("symbol", next.symbol);
    startTransition(() => {
      router.push(`/ice-pricing?${sp.toString()}`);
    });
  }

  const symbolList = useMemo(
    () => (symbols.includes(symbol) ? symbols : [symbol, ...symbols]),
    [symbol, symbols],
  );
  const groups = useMemo(() => groupSymbolsForPicker(symbolList), [symbolList]);

  return (
    <div className="flex flex-wrap items-end gap-4">
      <label className="flex flex-col gap-1 text-xs text-gray-400">
        Trade date
        <input
          type="date"
          value={tradeDate}
          onChange={(e) => update({ trade_date: e.target.value })}
          className="rounded border border-gray-700 bg-[#161a23] px-2 py-1 text-sm text-gray-100 focus:border-blue-400 focus:outline-none"
        />
      </label>
      <label className="flex flex-col gap-1 text-xs text-gray-400">
        Symbol
        <select
          value={symbol}
          onChange={(e) => update({ symbol: e.target.value })}
          disabled={symbolList.length === 0}
          className="min-w-[18rem] rounded border border-gray-700 bg-[#161a23] px-2 py-1 text-sm text-gray-100 focus:border-blue-400 focus:outline-none disabled:opacity-50"
        >
          {symbolList.length === 0 ? (
            <option value="">no symbols</option>
          ) : (
            groups.map((g) => (
              <optgroup key={g.contract_type} label={g.label}>
                {g.entries.map((e) => (
                  <option key={e.symbol} value={e.symbol}>
                    {e.symbol}
                    {e.description !== e.symbol ? ` — ${e.description}` : ""}
                  </option>
                ))}
              </optgroup>
            ))
          )}
        </select>
      </label>
      {pending ? (
        <span className="pb-1.5 text-xs text-gray-500">loading…</span>
      ) : null}
    </div>
  );
}
