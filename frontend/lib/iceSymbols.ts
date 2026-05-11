// PJM ICE short-term symbol registry — TS mirror of
// backend/scrapes/ice_python/symbols/pjm_short_term_symbols.py.
// Keep the two in sync: the backend registry drives ingestion, this
// one drives the frontend's symbol picker labels and grouping.

export type IceContractType =
  | "daily"
  | "weekly"
  | "weekend"
  | "balmo"
  | "monthly"
  | "quarterly"
  | "yearly";

export type IceProductType = "power" | "gas";

export interface IceSymbolEntry {
  symbol: string;
  description: string;
  product_type: IceProductType;
  contract_type: IceContractType;
}

export const PJM_SYMBOLS: IceSymbolEntry[] = [
  // Daily power
  { symbol: "PDP D0-IUS", description: "PJM Balance of Day", product_type: "power", contract_type: "daily" },
  { symbol: "PDP D1-IUS", description: "PJM RT Next Day",     product_type: "power", contract_type: "daily" },
  { symbol: "PDA D1-IUS", description: "PJM DA Next Day",     product_type: "power", contract_type: "daily" },
  // Weekly power
  { symbol: "PDP W0-IUS", description: "PJM Balance of Week", product_type: "power", contract_type: "weekly" },
  { symbol: "PDP W1-IUS", description: "PJM Week 1",          product_type: "power", contract_type: "weekly" },
  { symbol: "PDP W2-IUS", description: "PJM Week 2",          product_type: "power", contract_type: "weekly" },
  { symbol: "PDP W3-IUS", description: "PJM Week 3",          product_type: "power", contract_type: "weekly" },
  { symbol: "PDP W4-IUS", description: "PJM Week 4",          product_type: "power", contract_type: "weekly" },
  // Weekend power
  { symbol: "PDO P1-IUS", description: "PJM WH DA Off-Peak Weekend 2x16", product_type: "power", contract_type: "weekend" },
  { symbol: "ODP P1-IUS", description: "PJM WH RT Off-Peak Weekend 2x16", product_type: "power", contract_type: "weekend" },
];

// Display order for groupings — matches the comment-section order in the
// Python registry. Anything not in this list goes to the end.
export const CONTRACT_TYPE_ORDER: readonly IceContractType[] = [
  "daily",
  "balmo",
  "weekly",
  "weekend",
  "monthly",
  "quarterly",
  "yearly",
] as const;

const CONTRACT_TYPE_LABEL: Record<IceContractType, string> = {
  daily: "Daily",
  balmo: "Balance of Month",
  weekly: "Weekly",
  weekend: "Weekend",
  monthly: "Monthly",
  quarterly: "Quarterly",
  yearly: "Yearly",
};

export function getPjmSymbolMap(): Record<string, IceSymbolEntry> {
  return Object.fromEntries(PJM_SYMBOLS.map((e) => [e.symbol, e]));
}

export function getPjmSymbolCodes(): string[] {
  return PJM_SYMBOLS.map((e) => e.symbol);
}

export function lookupSymbol(symbol: string): IceSymbolEntry | null {
  return getPjmSymbolMap()[symbol] ?? null;
}

export function describeSymbol(symbol: string): string {
  return lookupSymbol(symbol)?.description ?? symbol;
}

export function contractTypeLabel(t: string): string {
  return (CONTRACT_TYPE_LABEL as Record<string, string>)[t] ?? t;
}

export interface IceSymbolGroup {
  contract_type: string;
  label: string;
  entries: IceSymbolEntry[];
}

// Group a symbol list (codes) into ordered buckets by contract_type, using
// the registry where available and falling back to a synthetic "unknown"
// entry so DB-present-but-not-registered symbols still surface in the UI.
export function groupSymbolsForPicker(codes: ReadonlyArray<string>): IceSymbolGroup[] {
  const map = getPjmSymbolMap();
  const buckets = new Map<string, IceSymbolEntry[]>();

  for (const code of codes) {
    const entry: IceSymbolEntry = map[code] ?? {
      symbol: code,
      description: code,
      product_type: "power",
      contract_type: "monthly", // best-guess bucket for unknowns; UI label reflects "Other"
    };
    const ct = map[code] ? entry.contract_type : "unknown";
    const bucket = buckets.get(ct);
    if (bucket) {
      bucket.push(entry);
    } else {
      buckets.set(ct, [entry]);
    }
  }

  const orderIndex = new Map<string, number>(
    CONTRACT_TYPE_ORDER.map((t, i) => [t, i]),
  );
  const sortedKeys = Array.from(buckets.keys()).sort((a, b) => {
    const ai = orderIndex.get(a) ?? Number.MAX_SAFE_INTEGER;
    const bi = orderIndex.get(b) ?? Number.MAX_SAFE_INTEGER;
    if (ai !== bi) return ai - bi;
    return a.localeCompare(b);
  });

  return sortedKeys.map((ct) => ({
    contract_type: ct,
    label: ct === "unknown" ? "Other" : contractTypeLabel(ct),
    entries: (buckets.get(ct) ?? []).sort((a, b) =>
      a.symbol.localeCompare(b.symbol),
    ),
  }));
}
