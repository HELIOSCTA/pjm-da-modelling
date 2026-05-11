"use client";

import Link from "next/link";

interface TabDef {
  key: string;
  label: string;
  enabled: boolean;
}

const TABS: TabDef[] = [
  { key: "day-ahead", label: "Day Ahead", enabled: true },
  { key: "balance-of-month", label: "Balance of Month", enabled: false },
  { key: "prompt-month", label: "Prompt Month", enabled: false },
];

export function Tabs({ active }: { active: string }) {
  return (
    <div className="border-b border-gray-800">
      <nav className="-mb-px flex gap-1">
        {TABS.map((t) => {
          const isActive = t.key === active;
          const base =
            "px-4 py-2 text-sm border-b-2 transition";
          if (!t.enabled) {
            return (
              <span
                key={t.key}
                title="Coming soon"
                className={`${base} cursor-not-allowed border-transparent text-gray-600`}
              >
                {t.label}
                <span className="ml-2 rounded bg-gray-800/60 px-1.5 py-0.5 text-[9px] uppercase tracking-wider text-gray-500">
                  soon
                </span>
              </span>
            );
          }
          return (
            <Link
              key={t.key}
              href={`/ice-pricing?tab=${t.key}`}
              className={`${base} ${
                isActive
                  ? "border-blue-400 text-gray-50"
                  : "border-transparent text-gray-400 hover:text-gray-200"
              }`}
            >
              {t.label}
            </Link>
          );
        })}
      </nav>
    </div>
  );
}
