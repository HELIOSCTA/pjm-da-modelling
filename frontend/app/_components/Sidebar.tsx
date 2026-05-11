"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { ComingSoonChip } from "./ComingSoonChip";

interface NavItem {
  href: string;
  label: string;
  icon: string;
  enabled: boolean;
  hint?: string;
}

// v1 nav is DA-Forecast-only. The ICE Pricing route (and any future
// sections) still resolve by URL; they're just not advertised here yet.
// The disabled-item rendering below is kept so re-adding a "soon" entry
// is a one-line change.
const NAV: NavItem[] = [
  { href: "/", label: "Home", icon: "◈", enabled: true },
  { href: "/forecast", label: "DA Forecast", icon: "▣", enabled: true },
];

function envMarker(): string {
  // NEXT_PUBLIC_VERCEL_ENV is set by Vercel automatically: "production",
  // "preview", "development". Falls through to localhost when running
  // `next dev` outside a Vercel build.
  const env = process.env.NEXT_PUBLIC_VERCEL_ENV;
  if (env === "production") return "v1 · production";
  if (env === "preview") return "v1 · preview";
  return "v0 · localhost";
}

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="flex h-screen w-56 shrink-0 flex-col border-r border-gray-800 bg-[#0b0d13]">
      <div className="border-b border-gray-800 px-4 py-4">
        <p className="text-sm font-semibold tracking-tight text-gray-100">
          PJM DA
        </p>
        <p className="text-[11px] uppercase tracking-wider text-gray-500">
          Operator Console
        </p>
      </div>
      <nav className="flex-1 overflow-y-auto px-2 py-3">
        <ul className="space-y-0.5">
          {NAV.map((item) => {
            const active =
              item.href === "/"
                ? pathname === "/"
                : pathname === item.href || pathname.startsWith(`${item.href}/`);
            const baseClass =
              "flex items-center justify-between gap-2 rounded px-3 py-2 text-sm";
            if (!item.enabled) {
              return (
                <li key={item.href}>
                  <span
                    className={`${baseClass} cursor-not-allowed text-gray-600`}
                    title="Coming soon"
                  >
                    <span className="flex items-center gap-2">
                      <span className="w-4 text-center">{item.icon}</span>
                      {item.label}
                    </span>
                    {item.hint ? <ComingSoonChip label={item.hint} /> : null}
                  </span>
                </li>
              );
            }
            return (
              <li key={item.href}>
                <Link
                  href={item.href}
                  className={`${baseClass} ${
                    active
                      ? "bg-blue-500/15 text-gray-50"
                      : "text-gray-400 hover:bg-gray-800/40 hover:text-gray-200"
                  }`}
                >
                  <span className="flex items-center gap-2">
                    <span
                      className={`w-4 text-center ${active ? "text-blue-300" : "text-gray-500"}`}
                    >
                      {item.icon}
                    </span>
                    {item.label}
                  </span>
                </Link>
              </li>
            );
          })}
        </ul>
      </nav>
      <div className="border-t border-gray-800 px-4 py-3 text-[10px] uppercase tracking-wider text-gray-600">
        {envMarker()}
      </div>
    </aside>
  );
}
