import {
  fetchEodRow,
  fetchLatestTradeDateWithEod,
  fetchSymbolsForDate,
  fetchTickRows,
} from "@/lib/server/icePricing";

import { DayAheadControls } from "./_components/DayAheadControls";
import { EodSummaryCard } from "./_components/EodSummaryCard";
import { IntradaySpark } from "./_components/IntradaySpark";
import { Tabs } from "./_components/Tabs";
import { TickBlotter } from "./_components/TickBlotter";

export const dynamic = "force-dynamic";

const DEFAULT_SYMBOL = "PDP D0-IUS";

function yesterdayUtc(): string {
  const t = new Date();
  t.setUTCDate(t.getUTCDate() - 1);
  return t.toISOString().slice(0, 10);
}

function pickString(
  v: string | string[] | undefined,
  fallback: string,
): string {
  if (typeof v === "string" && v.length > 0) return v;
  return fallback;
}

export default async function IcePricingPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const tab = pickString(params.tab, "day-ahead");
  const requestedSymbol = pickString(params.symbol, DEFAULT_SYMBOL);
  const explicitTradeDate =
    typeof params.trade_date === "string" && params.trade_date.length > 0
      ? params.trade_date
      : null;
  // Land on the most recent trade_date that actually has an EOD row
  // for the default symbol — falls back to yesterday-UTC only if the
  // table is empty (cold start). The user can still override via URL.
  const tradeDate =
    explicitTradeDate ??
    (await fetchLatestTradeDateWithEod(DEFAULT_SYMBOL)) ??
    yesterdayUtc();

  const symbols = await fetchSymbolsForDate(tradeDate);
  const symbol =
    symbols.includes(requestedSymbol)
      ? requestedSymbol
      : symbols.includes(DEFAULT_SYMBOL)
        ? DEFAULT_SYMBOL
        : (symbols[0] ?? requestedSymbol);

  const [eod, ticks] = symbols.length
    ? await Promise.all([
        fetchEodRow(tradeDate, symbol),
        fetchTickRows(tradeDate, symbol),
      ])
    : [null, []];

  return (
    <main className="px-8 py-8">
      <header className="mb-6">
        <h1 className="text-xl font-semibold tracking-tight">ICE Pricing</h1>
        <p className="mt-1 text-sm text-gray-400">
          ICE PJM ticker EOD aggregates and tick-level fills.
        </p>
      </header>

      <Tabs active={tab} />

      {tab === "day-ahead" ? (
        <div className="mt-6 space-y-6">
          <DayAheadControls
            tradeDate={tradeDate}
            symbol={symbol}
            symbols={symbols}
          />

          {symbols.length === 0 ? (
            <div className="rounded border border-gray-800 bg-[#161a23] p-6 text-sm text-gray-300">
              <p>No ICE EOD rows for {tradeDate}.</p>
              <p className="mt-2 text-gray-500">
                Pick another trade date — EOD typically lands after the ICE
                close, so today&apos;s date may not be available yet.
              </p>
            </div>
          ) : eod ? (
            <>
              <EodSummaryCard row={eod} />
              <IntradaySpark rows={ticks} />
              <TickBlotter rows={ticks} />
            </>
          ) : (
            <div className="rounded border border-gray-800 bg-[#161a23] p-6 text-sm text-gray-300">
              No EOD row for symbol{" "}
              <span className="font-mono">{symbol}</span> on {tradeDate}.
            </div>
          )}
        </div>
      ) : (
        <div className="mt-6 rounded border border-gray-800 bg-[#161a23] p-6 text-sm text-gray-400">
          Tab not yet implemented.
        </div>
      )}
    </main>
  );
}
