import Link from "next/link";

import { TAB_DEFS, type TabKey } from "../_lib/tabs";

// Server component — active state is computed from the URL pathname
// passed in by the layout. No client JS needed.
export function TabBar({ active }: { active: TabKey }) {
  return (
    <nav className="border-b border-gray-800 bg-[#0d1017]">
      <div className="flex gap-1 px-8">
        {TAB_DEFS.map((tab) => {
          const isActive = tab.key === active;
          return (
            <Link
              key={tab.key}
              href={tab.href}
              className={
                "border-b-2 px-4 py-3 text-sm font-medium transition-colors " +
                (isActive
                  ? "border-emerald-400 text-emerald-300"
                  : "border-transparent text-gray-400 hover:text-gray-200")
              }
            >
              {tab.label}
            </Link>
          );
        })}
      </div>
    </nav>
  );
}
