import type { IceAnchor } from "@/types/forecast";

function fmtMoney(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return "—";
  const sign = v >= 0 ? "+" : "";
  return `${sign}${v.toFixed(digits)}`;
}

function fmtInt(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return Math.round(v).toLocaleString();
}

// Header card for the ICE anchor block. Always renders — when the
// anchor wasn't applied (no eligible trades, or no usable OnPeak),
// the card still shows symbol + cutoff + a clear "raw fallback" badge.
export function IceAnchorCard({ anchor }: { anchor: IceAnchor }) {
  const applied = anchor.applied;
  return (
    <section
      className={
        "rounded-lg border p-4 " +
        (applied
          ? "border-emerald-700/60 bg-emerald-900/10"
          : "border-amber-700/60 bg-amber-900/10")
      }
    >
      <header className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
        <div className="flex items-baseline gap-3">
          <h2 className="text-sm font-semibold text-gray-100">ICE Anchor</h2>
          <span className="font-mono text-xs text-gray-400">{anchor.symbol}</span>
        </div>
        <span
          className={
            "rounded-full px-2 py-0.5 text-xs font-medium " +
            (applied
              ? "bg-emerald-800/40 text-emerald-200"
              : "bg-amber-800/40 text-amber-200")
          }
        >
          {applied
            ? `applied · anchored to ${anchor.anchor_label ?? "?"}`
            : "not applied · raw Meteo fallback"}
        </span>
      </header>

      <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm sm:grid-cols-4">
        <div>
          <dt className="text-xs text-gray-500">VWAP</dt>
          <dd className="font-mono text-gray-200">
            ${fmtMoney(anchor.vwap)} <span className="text-xs text-gray-500">/MWh</span>
          </dd>
        </div>
        <div>
          <dt className="text-xs text-gray-500">Volume</dt>
          <dd className="font-mono text-gray-200">
            {fmtInt(anchor.volume)} <span className="text-xs text-gray-500">MWh</span>
          </dd>
        </div>
        <div>
          <dt className="text-xs text-gray-500">Trades</dt>
          <dd className="font-mono text-gray-200">
            {anchor.n_trades}{" "}
            {anchor.n_excluded > 0 ? (
              <span className="text-xs text-gray-500">({anchor.n_excluded} excl)</span>
            ) : null}
          </dd>
        </div>
        <div>
          <dt className="text-xs text-gray-500">Active scale</dt>
          <dd className="font-mono text-gray-200">
            {anchor.shared_scale != null
              ? anchor.shared_scale.toFixed(4)
              : "—"}
          </dd>
        </div>
      </dl>

      {anchor.last_price != null ? (
        <p className="mt-3 text-xs text-gray-500">
          Last fill ${fmtMoney(anchor.last_price)} @{" "}
          <span className="font-mono">{anchor.last_time_local ?? "—"}</span>
        </p>
      ) : null}
    </section>
  );
}
