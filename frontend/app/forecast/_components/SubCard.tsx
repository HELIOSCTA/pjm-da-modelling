"use client";

import { useState, type ReactNode } from "react";

export function SubCard({
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
    <section className="rounded border border-gray-800/60 bg-gray-900/20">
      <button
        type="button"
        onClick={toggle}
        aria-expanded={open}
        className="flex w-full items-center justify-between px-4 py-2.5 text-left hover:bg-gray-900/40"
      >
        <div className="flex items-center gap-3">
          <h3 className="text-xs font-semibold uppercase tracking-wider text-gray-400">
            {title}
          </h3>
          {badge ? (
            <span className="rounded bg-gray-800/60 px-1.5 py-0.5 text-[9px] uppercase tracking-wider text-gray-500">
              {badge}
            </span>
          ) : null}
        </div>
        <svg
          className={`h-3.5 w-3.5 text-gray-500 transition-transform ${
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
      {open ? (
        <div className="border-t border-gray-800/60 px-4 py-4">{children}</div>
      ) : null}
    </section>
  );
}
