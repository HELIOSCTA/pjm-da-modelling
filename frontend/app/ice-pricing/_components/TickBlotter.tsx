"use client";

import { useMemo, useState } from "react";

import type { IceTickRow } from "@/types/icePricing";

type Direction = "Lift" | "Hit" | "Leg";
const DIRECTIONS: Direction[] = ["Lift", "Hit", "Leg"];

function fmtPrice(v: number | null): string {
  if (v === null || v === undefined) return "—";
  return Number(v).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  });
}

function fmtQty(v: number | null): string {
  if (v === null || v === undefined) return "—";
  return Number(v).toLocaleString();
}

function fmtTime(iso: string): string {
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return d.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  } catch {
    return iso;
  }
}

function directionClass(dir: string | null): string {
  switch (dir) {
    case "Lift":
      return "text-emerald-300";
    case "Hit":
      return "text-rose-300";
    case "Leg":
      return "text-amber-300";
    default:
      return "text-gray-500";
  }
}

function toggleClass(active: boolean, dir: Direction): string {
  if (!active) return "border-gray-800 text-gray-600";
  switch (dir) {
    case "Lift":
      return "border-emerald-500/40 text-emerald-300";
    case "Hit":
      return "border-rose-500/40 text-rose-300";
    case "Leg":
      return "border-amber-500/40 text-amber-300";
  }
}

interface TickRowDecorated {
  row: IceTickRow;
  arrow: "" | "↑" | "↓";
  arrowClass: string;
}

// Compute price-direction arrow vs the chronologically previous tick
// before reversing for display. Excludes Leg fills from the comparison
// (their price is a spread leg, not the level signal).
function decorate(rows: IceTickRow[]): TickRowDecorated[] {
  const decorated: TickRowDecorated[] = [];
  let prevPrice: number | null = null;
  for (const row of rows) {
    const price = row.price;
    let arrow: "" | "↑" | "↓" = "";
    let arrowClass = "text-gray-600";
    if (
      row.trade_direction !== "Leg" &&
      price !== null &&
      prevPrice !== null
    ) {
      if (price > prevPrice) {
        arrow = "↑";
        arrowClass = "text-emerald-400";
      } else if (price < prevPrice) {
        arrow = "↓";
        arrowClass = "text-rose-400";
      }
    }
    decorated.push({ row, arrow, arrowClass });
    if (row.trade_direction !== "Leg" && price !== null) {
      prevPrice = price;
    }
  }
  return decorated;
}

export function TickBlotter({ rows }: { rows: IceTickRow[] }) {
  const [enabled, setEnabled] = useState<Record<Direction, boolean>>({
    Lift: true,
    Hit: true,
    Leg: true,
  });

  // Decorate against the full chronological series first (so arrow
  // direction reflects real price action), then filter + reverse for
  // display.
  const visible = useMemo(() => {
    const decorated = decorate(rows);
    return decorated
      .filter(({ row }) => {
        const d = row.trade_direction as Direction | null;
        if (d === null) return true;
        return enabled[d] ?? true;
      })
      .reverse();
  }, [rows, enabled]);

  if (rows.length === 0) {
    return (
      <div className="rounded border border-gray-800 bg-[#161a23] p-4 text-sm text-gray-400">
        No tick-level fills for this date / symbol.
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded border border-gray-800 bg-[#161a23]">
      <div className="flex items-center justify-between gap-3 border-b border-gray-800 px-3 py-1.5">
        <span className="text-[10px] uppercase tracking-wider text-gray-500">
          Trade blotter · most recent first
        </span>
        <div className="flex items-center gap-1.5">
          {DIRECTIONS.map((dir) => (
            <button
              key={dir}
              type="button"
              onClick={() =>
                setEnabled((prev) => ({ ...prev, [dir]: !prev[dir] }))
              }
              className={`rounded border px-1.5 py-0.5 text-[10px] uppercase tracking-wider transition ${toggleClass(
                enabled[dir],
                dir,
              )}`}
              aria-pressed={enabled[dir]}
            >
              {dir}
            </button>
          ))}
          <span className="ml-2 text-[10px] text-gray-500">
            {visible.length}/{rows.length}
          </span>
        </div>
      </div>
      <div className="max-h-[640px] overflow-auto">
        <table className="w-full text-xs">
          <thead className="sticky top-0 z-10 bg-[#161a23] text-[10px] uppercase tracking-wider text-gray-500">
            <tr className="border-b border-gray-800">
              <th className="px-3 py-1.5 text-right font-normal">Time</th>
              <th className="px-3 py-1.5 text-left font-normal">Act</th>
              <th className="px-3 py-1.5 text-right font-normal">Price</th>
              <th className="px-3 py-1.5 text-center font-normal w-6"></th>
              <th className="px-3 py-1.5 text-right font-normal">Qty</th>
            </tr>
          </thead>
          <tbody className="font-mono">
            {visible.map(({ row, arrow, arrowClass }, i) => (
              <tr
                key={`${row.exec_time_local}-${i}`}
                className={`border-b border-gray-800/40 ${
                  i % 2 === 0 ? "bg-transparent" : "bg-gray-900/30"
                } hover:bg-gray-800/40`}
              >
                <td className="px-3 py-0.5 text-right text-gray-400 tabular-nums">
                  {fmtTime(row.exec_time_local)}
                </td>
                <td
                  className={`px-3 py-0.5 ${directionClass(row.trade_direction)}`}
                >
                  {row.trade_direction ?? "—"}
                </td>
                <td className="px-3 py-0.5 text-right tabular-nums text-gray-100">
                  {fmtPrice(row.price)}
                </td>
                <td
                  className={`px-1 py-0.5 text-center ${arrowClass}`}
                  aria-hidden="true"
                >
                  {arrow}
                </td>
                <td className="px-3 py-0.5 text-right tabular-nums text-gray-300">
                  {fmtQty(row.quantity)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
