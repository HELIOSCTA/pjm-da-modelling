"use client";

import { useState, type ReactNode } from "react";

export function CollapsibleCard({
  title,
  badge,
  defaultOpen = true,
  children,
}: {
  title: string;
  badge?: string;
  defaultOpen?: boolean;
  children: ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
  const toggle = () => setOpen((v) => !v);

  return (
    <section className="overflow-hidden rounded-lg border border-gray-800 bg-gray-900/40">
      <button
        type="button"
        onClick={toggle}
        aria-expanded={open}
        className="flex w-full items-center justify-between px-6 py-4 text-left hover:bg-gray-900/60"
      >
        <div className="flex items-center gap-3">
          <h2 className="text-sm font-medium uppercase tracking-wide text-gray-300">
            {title}
          </h2>
          {badge ? (
            <span className="rounded bg-gray-800/60 px-2 py-0.5 text-[10px] uppercase tracking-wider text-gray-400">
              {badge}
            </span>
          ) : null}
        </div>
        <svg
          className={`h-4 w-4 text-gray-400 transition-transform ${
            open ? "rotate-90" : "rotate-0"
          }`}
          viewBox="0 0 20 20"
          fill="currentColor"
          aria-hidden="true"
        >
          <path
            fillRule="evenodd"
            d="M7.21 14.77a.75.75 0 0 1 .02-1.06L11.168 10 7.23 6.29a.75.75 0 1 1 1.04-1.08l4.5 4.25a.75.75 0 0 1 0 1.08l-4.5 4.25a.75.75 0 0 1-1.06-.02Z"
            clipRule="evenodd"
          />
        </svg>
      </button>
      {open ? <div className="border-t border-gray-800 px-6 py-6">{children}</div> : null}
    </section>
  );
}
