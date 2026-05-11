import "server-only";

import { query } from "./db";
import type { IceEodRow, IceTickRow } from "@/types/icePricing";

const EOD_TABLE = "pjm_da_modelling_cleaned.ice_python_ticker_data_eod";
const TICK_TABLE = "pjm_da_modelling_cleaned.ice_python_ticker_data";

// pg returns DATE/TIMESTAMP as JS Date objects; cast to text so they
// render directly as strings and the row interfaces stay simple.
// NUMERIC also comes back as a string by default — float8 cast keeps
// the interface `number | null`. Counts are bigint-safe via ::bigint
// then ::text-free integer cast (counts are well under 2^31 in practice;
// the ::int cast keeps them as JS numbers).
const EOD_COLUMNS = [
  "trade_date::text AS trade_date",
  "symbol",
  "description",
  "product_type",
  "contract_type",
  "strip",
  "start_date::text AS start_date",
  "end_date::text AS end_date",
  '"open"::float8 AS open',
  '"high"::float8 AS high',
  '"low"::float8 AS low',
  '"close"::float8 AS close',
  "volume::float8 AS volume",
  "vwap::float8 AS vwap",
  "trade_count",
  "lift_count",
  "hit_count",
  "leg_count",
  "buy_volume::float8 AS buy_volume",
  "sell_volume::float8 AS sell_volume",
  "leg_volume::float8 AS leg_volume",
  "block_trade_count",
  "block_volume::float8 AS block_volume",
] as const;

const TICK_COLUMNS = [
  "to_char(exec_time_local, 'YYYY-MM-DD\"T\"HH24:MI:SS') AS exec_time_local",
  "trade_date::text AS trade_date",
  "symbol",
  "description",
  "product_type",
  "contract_type",
  "strip",
  "start_date::text AS start_date",
  "end_date::text AS end_date",
  "price::float8 AS price",
  "quantity::float8 AS quantity",
  "trade_direction",
] as const;

// Most-recent trade_date that actually has an EOD row for the given
// symbol. Used as the landing default so first paint isn't a blank
// "no rows for today" panel — yesterday-UTC frequently lands before
// the ICE close has been ingested.
export async function fetchLatestTradeDateWithEod(
  symbol: string,
): Promise<string | null> {
  const rows = await query<{ trade_date: string }>(
    `SELECT trade_date::text AS trade_date
     FROM ${EOD_TABLE}
     WHERE symbol = $1
     ORDER BY trade_date DESC
     LIMIT 1`,
    [symbol],
  );
  return rows[0]?.trade_date ?? null;
}

export async function fetchSymbolsForDate(tradeDate: string): Promise<string[]> {
  const rows = await query<{ symbol: string }>(
    `SELECT DISTINCT symbol
     FROM ${EOD_TABLE}
     WHERE trade_date = $1
     ORDER BY symbol`,
    [tradeDate],
  );
  return rows.map((r) => r.symbol);
}

export async function fetchEodRow(
  tradeDate: string,
  symbol: string,
): Promise<IceEodRow | null> {
  const rows = await query<IceEodRow>(
    `SELECT ${EOD_COLUMNS.join(", ")}
     FROM ${EOD_TABLE}
     WHERE trade_date = $1 AND symbol = $2
     LIMIT 1`,
    [tradeDate, symbol],
  );
  return rows[0] ?? null;
}

export async function fetchTickRows(
  tradeDate: string,
  symbol: string,
): Promise<IceTickRow[]> {
  return query<IceTickRow>(
    `SELECT ${TICK_COLUMNS.join(", ")}
     FROM ${TICK_TABLE}
     WHERE trade_date = $1 AND symbol = $2
     ORDER BY exec_time_local ASC`,
    [tradeDate, symbol],
  );
}
