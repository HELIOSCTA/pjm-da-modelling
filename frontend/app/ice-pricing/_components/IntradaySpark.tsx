"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  AXIS_COLOR,
  FORECAST_LINE,
  GRID_COLOR,
  TICK_COLOR,
  TOOLTIP_BG,
  TOOLTIP_BORDER,
  TOOLTIP_FG,
} from "@/lib/chartConstants";
import type { IceTickRow } from "@/types/icePricing";

interface SparkPoint {
  t: number; // ms since epoch — recharts needs a numeric x for proper scaling
  price: number;
  label: string;
}

function toPoints(rows: IceTickRow[]): SparkPoint[] {
  const out: SparkPoint[] = [];
  for (const r of rows) {
    if (r.trade_direction === "Leg") continue;
    if (r.price === null || r.price === undefined) continue;
    const ms = Date.parse(r.exec_time_local);
    if (Number.isNaN(ms)) continue;
    out.push({
      t: ms,
      price: r.price,
      label: new Date(ms).toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      }),
    });
  }
  return out;
}

export function IntradaySpark({ rows }: { rows: IceTickRow[] }) {
  const points = toPoints(rows);
  if (points.length < 2) return null;

  return (
    <div className="overflow-hidden rounded border border-gray-800 bg-[#161a23]">
      <div className="flex items-baseline justify-between border-b border-gray-800 px-3 py-1.5">
        <span className="text-[10px] uppercase tracking-wider text-gray-500">
          Intraday · price vs time (excludes Leg fills)
        </span>
        <span className="text-[10px] text-gray-500">{points.length} ticks</span>
      </div>
      <div className="h-[150px] w-full">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={points}
            margin={{ top: 8, right: 12, bottom: 4, left: 4 }}
          >
            <CartesianGrid stroke={GRID_COLOR} strokeDasharray="3 3" />
            <XAxis
              dataKey="t"
              type="number"
              domain={["dataMin", "dataMax"]}
              tick={false}
              axisLine={{ stroke: AXIS_COLOR }}
              height={4}
            />
            <YAxis
              dataKey="price"
              tick={{ fill: TICK_COLOR, fontSize: 10 }}
              stroke={AXIS_COLOR}
              tickFormatter={(v) => Number(v).toFixed(2)}
              width={48}
              domain={["dataMin", "dataMax"]}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: TOOLTIP_BG,
                border: `1px solid ${TOOLTIP_BORDER}`,
                color: TOOLTIP_FG,
                fontSize: 11,
              }}
              labelFormatter={(v) =>
                new Date(Number(v)).toLocaleTimeString("en-US", {
                  hour: "2-digit",
                  minute: "2-digit",
                  second: "2-digit",
                  hour12: false,
                })
              }
              formatter={(value) =>
                value == null ? "—" : Number(value).toFixed(4)
              }
            />
            <Line
              type="monotone"
              dataKey="price"
              stroke={FORECAST_LINE}
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
