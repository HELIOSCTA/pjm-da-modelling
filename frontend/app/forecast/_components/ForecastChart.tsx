"use client";

import { useEffect, useMemo, useRef, useState } from "react";
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
  MEDIAN_LINE,
  MEDIAN_LINE_WIDTH,
  ONPEAK_HE_END,
  ONPEAK_HE_START,
  ONPEAK_SHADE_FILL,
  ONPEAK_SHADE_OPACITY,
  TABLE_BAND_COL_FRACTION,
  TABLE_BLOCKS_AREA_FRACTION,
  TICK_COLOR,
  TOOLTIP_BG,
  TOOLTIP_BORDER,
  TOOLTIP_FG,
} from "@/lib/chartConstants";
import type { HourlyForecastEntry } from "@/types/forecast";

interface ChartRow {
  hour_ending: number;
  point_forecast: number | null;
  median: number | null;
  actual_lmp: number | null;
  band_p10_p90: [number, number] | null;
  band_p25_p75: [number, number] | null;
}

const SERIES = {
  P10_P90: "P10–P90",
  P25_P75: "P25–P75",
  P50: "P50 (median)",
  FORECAST: "Forecast",
  ACTUAL: "Actual",
} as const;

function toChartRows(hourly: HourlyForecastEntry[]): ChartRow[] {
  return hourly.map((h) => ({
    hour_ending: h.hour_ending,
    point_forecast: h.point_forecast,
    median: h.q50,
    actual_lmp: h.actual_lmp,
    band_p10_p90:
      h.q10 !== null && h.q90 !== null ? [h.q10, h.q90] : null,
    band_p25_p75:
      h.q25 !== null && h.q75 !== null ? [h.q25, h.q75] : null,
  }));
}

function ChartBody({
  rows,
  hasActuals,
  hidden,
  onLegendClick,
  containerWidth,
}: {
  rows: ChartRow[];
  hasActuals: boolean;
  hidden: Set<string>;
  onLegendClick: (name: string) => void;
  containerWidth: number;
}) {
  // Align HE1..HE24 chart-tick centers with the table's HE column centers.
  // Recharts category XAxis already places each tick at its slot's center,
  // so we just need plot.x = end of the table's Band column and plot.right
  // = start of the table's OnPeak block column. Then slot[i].center == HE
  // column[i].center for free.
  //
  // Recharts left offset = margin.left + YAxis.width. Stash a small buffer
  // in margin.left and let YAxis own the rest, so its tick labels render
  // inside the space the table reserves for the Band column (not pushed
  // into the plot area).
  const leftOffsetPx = Math.max(36, Math.round(containerWidth * TABLE_BAND_COL_FRACTION));
  const rightOffsetPx = Math.max(8, Math.round(containerWidth * TABLE_BLOCKS_AREA_FRACTION));
  const yAxisWidth = Math.max(28, leftOffsetPx - 4);
  return (
    <ResponsiveContainer width="100%" height="100%">
      <ComposedChart
        data={rows}
        margin={{ top: 16, right: rightOffsetPx, left: 4, bottom: 24 }}
      >
        <CartesianGrid stroke={GRID_COLOR} strokeDasharray="3 3" />
        <XAxis
          dataKey="hour_ending"
          type="category"
          interval={0}
          padding={{ left: 0, right: 0 }}
          tick={{ fill: TICK_COLOR, fontSize: 11 }}
          stroke={AXIS_COLOR}
          label={{
            value: "Hour Ending",
            position: "insideBottom",
            offset: -2,
            fill: TICK_COLOR,
            fontSize: 12,
          }}
        />
        <YAxis
          tick={{ fill: TICK_COLOR, fontSize: 12 }}
          stroke={AXIS_COLOR}
          width={yAxisWidth}
          domain={[
            (min: number) => Math.floor(min - 2),
            (max: number) => Math.ceil(max + 2),
          ]}
          allowDataOverflow={false}
          label={{
            value: "$/MWh",
            angle: -90,
            position: "insideLeft",
            fill: TICK_COLOR,
            fontSize: 12,
          }}
        />
        <ReferenceArea
          x1={ONPEAK_HE_START}
          x2={ONPEAK_HE_END}
          fill={ONPEAK_SHADE_FILL}
          fillOpacity={ONPEAK_SHADE_OPACITY}
          stroke="none"
          ifOverflow="extendDomain"
          label={{
            value: "OnPeak",
            position: "insideTop",
            fill: TICK_COLOR,
            fontSize: 11,
            offset: 6,
          }}
        />
        <Tooltip
          contentStyle={{
            backgroundColor: TOOLTIP_BG,
            border: `1px solid ${TOOLTIP_BORDER}`,
            fontSize: 12,
          }}
          labelStyle={{ color: TOOLTIP_FG }}
          itemStyle={{ color: TOOLTIP_FG }}
          formatter={(value) => {
            if (value === undefined || value === null) return "—";
            if (Array.isArray(value)) {
              return value
                .map((v) => (typeof v === "number" ? v.toFixed(2) : String(v)))
                .join(" – ");
            }
            return typeof value === "number" ? value.toFixed(2) : String(value);
          }}
          labelFormatter={(he) => `HE${he}`}
        />
        <Legend
          verticalAlign="bottom"
          wrapperStyle={{
            color: TICK_COLOR,
            fontSize: 12,
            cursor: "pointer",
            paddingTop: 16,
          }}
          onClick={(entry) => {
            if (entry && typeof entry.value === "string") onLegendClick(entry.value);
          }}
          formatter={(value) => (
            <span
              style={{
                color: hidden.has(value) ? "#4b5563" : TOOLTIP_FG,
                textDecoration: hidden.has(value) ? "line-through" : "none",
              }}
            >
              {value}
            </span>
          )}
        />
        <Area
          type="monotone"
          dataKey="band_p10_p90"
          name={SERIES.P10_P90}
          stroke="none"
          fill={BAND_OUTER_FILL}
          fillOpacity={BAND_OUTER_OPACITY}
          isAnimationActive={false}
          hide={hidden.has(SERIES.P10_P90)}
        />
        <Area
          type="monotone"
          dataKey="band_p25_p75"
          name={SERIES.P25_P75}
          stroke="none"
          fill={BAND_INNER_FILL}
          fillOpacity={BAND_INNER_OPACITY}
          isAnimationActive={false}
          hide={hidden.has(SERIES.P25_P75)}
        />
        <Line
          type="monotone"
          dataKey="median"
          name={SERIES.P50}
          stroke={MEDIAN_LINE}
          strokeWidth={MEDIAN_LINE_WIDTH}
          strokeDasharray="2 4"
          dot={false}
          isAnimationActive={false}
          hide={hidden.has(SERIES.P50)}
        />
        <Line
          type="monotone"
          dataKey="point_forecast"
          name={SERIES.FORECAST}
          stroke={FORECAST_LINE}
          strokeWidth={FORECAST_LINE_WIDTH}
          dot={false}
          isAnimationActive={false}
          hide={hidden.has(SERIES.FORECAST)}
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
            hide={hidden.has(SERIES.ACTUAL)}
          />
        ) : null}
      </ComposedChart>
    </ResponsiveContainer>
  );
}

function useContainerWidth() {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    const node = ref.current;
    if (!node) return;
    const obs = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width ?? node.clientWidth;
      setWidth(w);
    });
    obs.observe(node);
    setWidth(node.clientWidth);
    return () => obs.disconnect();
  }, []);
  return { ref, width };
}

export function ForecastChart({ hourly }: { hourly: HourlyForecastEntry[] }) {
  const rows = useMemo(() => toChartRows(hourly), [hourly]);
  const hasActuals = rows.some((r) => r.actual_lmp !== null);
  // P50 hidden by default — Forecast (mean) is the primary point line; the
  // median is available via legend toggle for distribution-shape inspection.
  const [hidden, setHidden] = useState<Set<string>>(new Set([SERIES.P50]));
  const [focused, setFocused] = useState(false);
  const inline = useContainerWidth();
  const focus = useContainerWidth();

  const toggleSeries = (name: string) => {
    setHidden((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  // Esc to exit focus mode.
  useEffect(() => {
    if (!focused) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setFocused(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [focused]);

  return (
    <>
      <div ref={inline.ref} className="relative h-[420px] w-full">
        <button
          type="button"
          onClick={() => setFocused(true)}
          className="absolute right-2 top-2 z-10 rounded border border-gray-800 bg-gray-900/70 px-2 py-1 text-xs text-gray-300 hover:bg-gray-800 hover:text-gray-100"
          aria-label="Expand chart"
        >
          ⤢ Focus
        </button>
        <ChartBody
          rows={rows}
          hasActuals={hasActuals}
          hidden={hidden}
          onLegendClick={toggleSeries}
          containerWidth={inline.width}
        />
      </div>
      {focused ? (
        <div
          className="fixed inset-0 z-50 flex flex-col bg-[#0f1117]/95 p-6 backdrop-blur"
          onClick={(e) => {
            if (e.target === e.currentTarget) setFocused(false);
          }}
        >
          <div className="mb-3 flex items-center justify-between">
            <span className="text-sm text-gray-400">
              Press <kbd className="rounded border border-gray-700 px-1.5 text-gray-300">Esc</kbd> to exit focus mode
            </span>
            <button
              type="button"
              onClick={() => setFocused(false)}
              className="rounded border border-gray-800 bg-gray-900/70 px-3 py-1 text-sm text-gray-300 hover:bg-gray-800 hover:text-gray-100"
            >
              ✕ Close
            </button>
          </div>
          <div ref={focus.ref} className="flex-1">
            <ChartBody
              rows={rows}
              hasActuals={hasActuals}
              hidden={hidden}
              onLegendClick={toggleSeries}
              containerWidth={focus.width}
            />
          </div>
        </div>
      ) : null}
    </>
  );
}
