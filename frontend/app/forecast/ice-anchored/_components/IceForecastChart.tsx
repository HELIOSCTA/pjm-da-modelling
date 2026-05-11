"use client";

import {
  Area,
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
  BAND_INNER_FILL,
  BAND_INNER_OPACITY,
  BAND_OUTER_FILL,
  BAND_OUTER_OPACITY,
  FORECAST_LINE,
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
import type { IceHourlyEntry } from "@/types/forecast";

interface ChartRow {
  hour_ending: number;
  point_forecast: number | null;
  actual_lmp: number | null;
  band_envelope: [number, number] | null; // ENS Bottom .. ENS Top
  band_members: [number, number] | null; // members p25 .. p75
}

const SERIES = {
  ENVELOPE: "ENS Bottom–Top",
  MEMBERS: "Members P25–P75",
  FORECAST: "Det",
  ACTUAL: "Actual",
} as const;

function toChartRows(hourly: IceHourlyEntry[]): ChartRow[] {
  return hourly.map((h) => ({
    hour_ending: h.hour_ending,
    point_forecast: h.point_forecast,
    actual_lmp: h.actual_lmp,
    band_envelope:
      h.ens_bottom != null && h.ens_top != null ? [h.ens_bottom, h.ens_top] : null,
    band_members:
      h.members_p25 != null && h.members_p75 != null
        ? [h.members_p25, h.members_p75]
        : null,
  }));
}

export function IceForecastChart({ hourly }: { hourly: IceHourlyEntry[] }) {
  const rows = toChartRows(hourly);
  const hasActuals = rows.some((r) => r.actual_lmp != null);

  return (
    <div className="h-80 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart
          data={rows}
          margin={{ top: 10, right: 16, bottom: 0, left: 0 }}
        >
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
            tickFormatter={(v) => `$${Math.round(v)}`}
            width={48}
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
            formatter={(value) => {
              if (Array.isArray(value)) {
                const [lo, hi] = value as Array<number | null>;
                return `${lo == null ? "—" : `$${Number(lo).toFixed(2)}`} … ${
                  hi == null ? "—" : `$${Number(hi).toFixed(2)}`
                }`;
              }
              return value == null ? "—" : `$${Number(value).toFixed(2)}`;
            }}
          />
          <Legend wrapperStyle={{ fontSize: 12, color: TICK_COLOR }} />

          <Area
            type="monotone"
            dataKey="band_envelope"
            name={SERIES.ENVELOPE}
            stroke="none"
            fill={BAND_OUTER_FILL}
            fillOpacity={BAND_OUTER_OPACITY}
            isAnimationActive={false}
          />
          <Area
            type="monotone"
            dataKey="band_members"
            name={SERIES.MEMBERS}
            stroke="none"
            fill={BAND_INNER_FILL}
            fillOpacity={BAND_INNER_OPACITY}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="point_forecast"
            name={SERIES.FORECAST}
            stroke={FORECAST_LINE}
            strokeWidth={FORECAST_LINE_WIDTH}
            dot={false}
            isAnimationActive={false}
          />
          {hasActuals ? (
            <Line
              type="monotone"
              dataKey="actual_lmp"
              name={SERIES.ACTUAL}
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
