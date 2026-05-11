import type { ReactNode } from "react";

import { ONPEAK_HE_END, ONPEAK_HE_START } from "@/lib/chartConstants";
import {
  HE_LIST,
  gradientCellClass,
  gradientRange,
  type GradientKind,
} from "@/lib/forecastMetrics";
import type { BlockName } from "@/types/forecast";

const ONPEAK_HE_SET = new Set(
  Array.from(
    { length: ONPEAK_HE_END - ONPEAK_HE_START + 1 },
    (_, i) => ONPEAK_HE_START + i,
  ),
);

const ONPEAK_CELL_OVERLAY = "bg-amber-500/10";
const ONPEAK_BORDER = "border-amber-500/60";
const ONPEAK_BANNER = "bg-amber-500/20 text-amber-200";

const BLOCK_COLS: BlockName[] = ["OnPeak", "OffPeak", "Flat"];

export interface MetricRow {
  label: string;
  className?: string;
  perHE: (string | null)[];
  perBlock: Record<BlockName, string | null>;
  perHEClass?: (string | null)[];
  perBlockClass?: Partial<Record<BlockName, string | null>>;
  gradient?: {
    kind: GradientKind;
    rawHE: (number | null)[];
    rawBlock: Record<BlockName, number | null>;
  };
  // When true, render a thicker bottom border on this row to visually
  // separate it from the next (e.g. outputs vs diagnostic metrics).
  dividerAfter?: boolean;
}

export function HourlyMetricTable({
  rows,
  footer,
  caption,
}: {
  rows: MetricRow[];
  footer?: ReactNode;
  caption?: ReactNode;
}) {
  return (
    <div className="overflow-hidden rounded border border-gray-800">
      {caption ? (
        <div className="border-b border-gray-800 bg-[#161a23] px-3 py-2 text-xs text-gray-400">
          {caption}
        </div>
      ) : null}
      <table className="w-full table-fixed border-collapse text-center text-[11px] tabular-nums">
        <colgroup>
          <col style={{ width: "6%" }} />
          {HE_LIST.map((he) => (
            <col key={`col-${he}`} style={{ width: `${94 / 27}%` }} />
          ))}
          {BLOCK_COLS.map((b) => (
            <col key={`col-${b}`} style={{ width: `${94 / 27}%` }} />
          ))}
        </colgroup>
        <thead>
          <tr className="bg-[#161a23] text-[10px] font-semibold uppercase tracking-wider">
            <th />
            <th colSpan={ONPEAK_HE_START - 1} />
            <th
              colSpan={ONPEAK_HE_END - ONPEAK_HE_START + 1}
              className={`${ONPEAK_BANNER} border-l border-r ${ONPEAK_BORDER} px-2 py-1`}
            >
              ONPEAK · HE{ONPEAK_HE_START}–{ONPEAK_HE_END}
            </th>
            <th />
            <th
              className={`${ONPEAK_BANNER} border-l border-r ${ONPEAK_BORDER} px-2 py-1`}
            >
              ONPEAK
            </th>
            <th colSpan={BLOCK_COLS.length - 1} />
          </tr>
          <tr className="border-b border-gray-800 bg-[#161a23] text-[10px] uppercase tracking-wide text-gray-400">
            <th className="px-2 py-2 text-left font-medium">Row</th>
            {HE_LIST.map((he) => {
              const isOn = ONPEAK_HE_SET.has(he);
              return (
                <th
                  key={`he-${he}`}
                  className={`px-1 py-1 font-medium ${
                    isOn ? `${ONPEAK_CELL_OVERLAY} text-amber-100` : ""
                  } ${he === ONPEAK_HE_START ? `border-l ${ONPEAK_BORDER}` : ""} ${
                    he === ONPEAK_HE_END ? `border-r ${ONPEAK_BORDER}` : ""
                  }`}
                >
                  HE{he}
                </th>
              );
            })}
            {BLOCK_COLS.map((b) => {
              const isOn = b === "OnPeak";
              return (
                <th
                  key={`block-${b}`}
                  className={`px-2 py-1 font-medium ${
                    isOn
                      ? `${ONPEAK_CELL_OVERLAY} border-l border-r ${ONPEAK_BORDER} text-amber-100`
                      : "border-l border-gray-800 text-gray-300"
                  }`}
                >
                  {b}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => {
            const range = row.gradient
              ? gradientRange(row.gradient.rawHE)
              : null;
            const gradClass = (val: number | null) =>
              row.gradient && range
                ? gradientCellClass(val, range.lo, range.hi, row.gradient.kind)
                : "";
            const rowBorder = row.dividerAfter
              ? "border-b-2 border-gray-500"
              : "border-b border-gray-800/60";
            return (
              <tr
                key={`${row.label}-${ri}`}
                className={`${rowBorder} ${row.className ?? ""}`}
              >
                <td className="border-r border-gray-800 px-2 py-1 text-left">
                  {row.label}
                </td>
                {HE_LIST.map((he, i) => {
                  const cellGrad = row.gradient
                    ? gradClass(row.gradient.rawHE[i])
                    : "";
                  const cellOverride = row.perHEClass?.[i] ?? "";
                  // OnPeak fill is reserved for the column-header banner only;
                  // body cells stay neutral so the gradient shading reads
                  // cleanly. Vertical OnPeak borders still bracket the block.
                  return (
                    <td
                      key={`${row.label}-${he}`}
                      className={`px-1 py-1 ${
                        he === ONPEAK_HE_START ? `border-l ${ONPEAK_BORDER}` : ""
                      } ${he === ONPEAK_HE_END ? `border-r ${ONPEAK_BORDER}` : ""} ${cellGrad} ${cellOverride}`}
                    >
                      {row.perHE[i] ?? "—"}
                    </td>
                  );
                })}
                {BLOCK_COLS.map((b) => {
                  const isOn = b === "OnPeak";
                  const cellGrad = row.gradient
                    ? gradClass(row.gradient.rawBlock[b])
                    : "";
                  const cellOverride = row.perBlockClass?.[b] ?? "";
                  return (
                    <td
                      key={`${row.label}-${b}`}
                      className={`px-2 py-1 ${
                        isOn
                          ? `border-l border-r ${ONPEAK_BORDER}`
                          : "border-l border-gray-800"
                      } ${cellGrad} ${cellOverride}`}
                    >
                      {row.perBlock[b] ?? "—"}
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
      {footer ? (
        <div className="border-t border-gray-800 px-3 py-2 text-xs text-gray-300">
          {footer}
        </div>
      ) : null}
    </div>
  );
}
