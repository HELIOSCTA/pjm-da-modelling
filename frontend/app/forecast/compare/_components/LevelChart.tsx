"use client";

import {
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ReferenceArea,
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

export interface LevelSeries {
  key: string;
  label: string;
  color: string;
  values: Array<number | null>; // dense 24-length, index = HE - 1
}

const ACTUAL_KEY = "__actual";

type Row = { hour_ending: number } & Record<string, number | null>;

function toRows(series: LevelSeries[], actual: Array<number | null> | null): Row[] {
  const rows: Row[] = Array.from({ length: 24 }, (_, i) => ({ hour_ending: i + 1 }));
  for (const s of series) {
    for (let i = 0; i < 24; i++) rows[i][s.key] = s.values[i] ?? null;
  }
  if (actual) for (let i = 0; i < 24; i++) rows[i][ACTUAL_KEY] = actual[i] ?? null;
  return rows;
}

// Absolute $/MWh overlay (HE1–HE24) of each model's hourly point forecast,
// plus an optional Actual DA line. On-peak block shaded for reference.
export function LevelChart({
  series,
  actual,
}: {
  series: LevelSeries[];
  actual: Array<number | null> | null;
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
            tickFormatter={(v) => `$${Number(v).toFixed(0)}`}
            width={56}
            domain={["auto", "auto"]}
          />
          <ReferenceArea
            x1={ONPEAK_HE_START}
            x2={ONPEAK_HE_END}
            fill={ONPEAK_SHADE_FILL}
            fillOpacity={ONPEAK_SHADE_OPACITY}
            ifOverflow="extendDomain"
          />
          <Tooltip
            contentStyle={{
              backgroundColor: TOOLTIP_BG,
              border: `1px solid ${TOOLTIP_BORDER}`,
              color: TOOLTIP_FG,
              fontSize: 12,
            }}
            labelFormatter={(v) => `HE${v}`}
            formatter={(value) =>
              value == null ? "—" : `$${Number(value).toFixed(2)}`
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
