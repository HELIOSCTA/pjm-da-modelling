"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useTransition } from "react";

import { labelForRun } from "@/lib/forecastRunLabel";
import type { ForecastRunListEntry } from "@/types/forecast";

// URL-driven picker. Updating selection pushes a new value to ``paramKey``
// (or clears it for "Latest"). Per-model tabs use the default "run_id";
// the compare tab passes "run_<modelKey>" (e.g. "run_knn", "run_ice") so
// one picker per model can coexist on the same URL.
export function RunPicker({
  runs,
  activeRunId,
  paramKey = "run_id",
  label = "Run",
}: {
  runs: ForecastRunListEntry[];
  activeRunId: string | null;
  paramKey?: string;
  label?: string;
}) {
  const router = useRouter();
  const params = useSearchParams();
  const [pending, startTransition] = useTransition();

  function onChange(e: React.ChangeEvent<HTMLSelectElement>) {
    const next = new URLSearchParams(params.toString());
    const value = e.target.value;
    if (value) next.set(paramKey, value);
    else next.delete(paramKey);
    startTransition(() => router.replace(`?${next.toString()}`, { scroll: false }));
  }

  if (runs.length === 0) {
    return (
      <span className="text-xs text-gray-500">
        {label}: no runs for this date
      </span>
    );
  }

  return (
    <label className="flex items-center gap-2 text-xs text-gray-400">
      <span>{label}</span>
      <select
        value={activeRunId ?? ""}
        onChange={onChange}
        disabled={pending}
        className="rounded border border-gray-700 bg-[#161a23] px-2 py-1 text-xs text-gray-200 focus:border-emerald-500 focus:outline-none"
      >
        <option value="">Latest ({labelForRun(runs[0])})</option>
        {runs.map((r) => (
          <option key={r.run_id} value={r.run_id}>
            {labelForRun(r)} · {r.run_id.slice(0, 8)}
          </option>
        ))}
      </select>
    </label>
  );
}
