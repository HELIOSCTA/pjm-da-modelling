"use client";

import {
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  ACTUAL_DASH,
  ACTUAL_LINE,
  ACTUAL_LINE_WIDTH,
  AXIS_COLOR,
  FORECAST_LINE_WIDTH,
  GRID_COLOR,
  ONPEAK_HE_END,
  ONPEAK_HE_START,
  ONPEAK_SHADE_FILL,
  ONPEAK_SHADE_OPACITY,
  TICK_COLOR,
  TOOLTIP_BG,
  TOOLTIP_BORDER,
  TOOLTIP_FG,
} from "@/lib/chartConstants";

import type { ShapePoint } from "../../_lib/shape";

export interface ShapeSeries {
  key: string;
  label: string;
  color: string;
  points: ShapePoint[];
}

const ACTUAL_KEY = "__actual";

type Row = { hour_ending: number } & Record<string, number | null>;

function toRows(series: ShapeSeries[], actual: ShapePoint[] | null): Row[] {
  const rows: Row[] = Array.from({ length: 24 }, (_, i) => ({ hour_ending: i + 1 }));
  const apply = (key: string, points: ShapePoint[]) => {
    const byHe = new Map(points.map((p) => [p.hour_ending, p.ratio]));
    for (const r of rows) r[key] = byHe.get(r.hour_ending) ?? null;
  };
  for (const s of series) apply(s.key, s.points);
  if (actual) apply(ACTUAL_KEY, actual);
  return rows;
}

// Normalized shape overlay (each series ÷ its own 24-hour mean). N model
// series + an optional Actual line, all dimensionless ratios on one axis.
export function ShapeChart({
  series,
  actual,
}: {
  series: ShapeSeries[];
  actual: ShapePoint[] | null;
}) {
  const rows = toRows(series, actual);

  return (
    <div className="h-80 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={rows} margin={{ top: 10, right: 16, bottom: 0, left: 0 }}>
          <CartesianGrid stroke={GRID_COLOR} strokeDasharray="3 3" />
          <XAxis
            dataKey="hour_ending"
            tick={{ fill: TICK_COLOR, fontSize: 11 }}
            stroke={AXIS_COLOR}
            ticks={[1, 4, 8, 12, 16, 20, 24]}
          />
          <YAxis
            tick={{ fill: TICK_COLOR, fontSize: 11 }}
            stroke={AXIS_COLOR}
            tickFormatter={(v) => Number(v).toFixed(2)}
            width={48}
            domain={["auto", "auto"]}
          />
          <ReferenceArea
            x1={ONPEAK_HE_START}
            x2={ONPEAK_HE_END}
            fill={ONPEAK_SHADE_FILL}
            fillOpacity={ONPEAK_SHADE_OPACITY}
            ifOverflow="extendDomain"
          />
          <ReferenceLine y={1} stroke={AXIS_COLOR} strokeDasharray="2 2" />
          <Tooltip
            contentStyle={{
              backgroundColor: TOOLTIP_BG,
              border: `1px solid ${TOOLTIP_BORDER}`,
              color: TOOLTIP_FG,
              fontSize: 12,
            }}
            labelFormatter={(v) => `HE${v}`}
            formatter={(value) =>
              value == null ? "—" : `${Number(value).toFixed(3)}×`
            }
          />
          <Legend wrapperStyle={{ fontSize: 12, color: TICK_COLOR }} />

          {series.map((s) => (
            <Line
              key={s.key}
              type="monotone"
              dataKey={s.key}
              name={s.label}
              stroke={s.color}
              strokeWidth={FORECAST_LINE_WIDTH}
              dot={false}
              isAnimationActive={false}
            />
          ))}
          {actual ? (
            <Line
              type="monotone"
              dataKey={ACTUAL_KEY}
              name="Actual DA"
              stroke={ACTUAL_LINE}
              strokeWidth={ACTUAL_LINE_WIDTH}
              strokeDasharray={ACTUAL_DASH}
              dot={false}
              isAnimationActive={false}
            />
          ) : null}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
