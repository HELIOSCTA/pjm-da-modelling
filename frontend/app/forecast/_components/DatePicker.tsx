"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useTransition } from "react";

// URL-driven date picker. Changing the date drops any run_id since
// run_ids only make sense within a single (model, date).
export function DatePicker({
  dates,
  activeDate,
}: {
  dates: string[];
  activeDate: string;
}) {
  const router = useRouter();
  const params = useSearchParams();
  const [pending, startTransition] = useTransition();

  function onChange(e: React.ChangeEvent<HTMLSelectElement>) {
    const next = new URLSearchParams(params.toString());
    next.set("target_date", e.target.value);
    next.delete("run_id");
    startTransition(() => router.replace(`?${next.toString()}`, { scroll: false }));
  }

  // If the active date isn't in the list (e.g. defaulted to tomorrow,
  // no runs yet), still show it as a disabled-feeling sentinel option.
  const options = dates.includes(activeDate) ? dates : [activeDate, ...dates];

  return (
    <label className="flex items-center gap-2 text-xs text-gray-400">
      <span>Target</span>
      <select
        value={activeDate}
        onChange={onChange}
        disabled={pending}
        className="rounded border border-gray-700 bg-[#161a23] px-2 py-1 text-xs text-gray-200 focus:border-emerald-500 focus:outline-none"
      >
        {options.map((d) => (
          <option key={d} value={d}>
            {d}
          </option>
        ))}
      </select>
    </label>
  );
}
