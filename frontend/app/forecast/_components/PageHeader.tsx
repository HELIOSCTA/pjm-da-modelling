import type { ReactNode } from "react";

// Standard header for every /forecast/* tab. Title + optional subline
// on the left, optional metadata block on the right. Pages compose the
// right side via a render prop so the same shell handles single-run
// tabs (Like-Day / ICE) and multi-run tabs (Compare).
export function PageHeader({
  title,
  subline,
  rightMetadata,
}: {
  title: ReactNode;
  subline?: ReactNode;
  rightMetadata?: ReactNode;
}) {
  return (
    <header className="mb-6 flex flex-wrap items-baseline justify-between gap-y-2">
      <div>
        <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
        {subline ? (
          <p className="mt-1 text-sm text-gray-400">{subline}</p>
        ) : null}
      </div>
      {rightMetadata ? (
        <div className="text-right text-xs text-gray-500">{rightMetadata}</div>
      ) : null}
    </header>
  );
}

// Convenience renderer for the canonical "Generated {ts} ET · run {id}"
// block used by the per-model tabs. Compare tab calls this once per
// model and stacks them.
export function RunMetadata({
  createdAtUtc,
  runId,
  label,
}: {
  createdAtUtc: string;
  runId: string;
  label?: string;
}) {
  return (
    <div>
      {label ? (
        <span className="mr-2 font-semibold text-gray-400">{label}</span>
      ) : null}
      Generated {formatTimestampEt(createdAtUtc)} ET
      <br />
      run <span className="font-mono">{runId.slice(0, 8)}</span>
    </div>
  );
}

function formatTimestampEt(iso: string): string {
  try {
    return new Date(iso).toLocaleString("en-US", {
      timeZone: "America/New_York",
      dateStyle: "medium",
      timeStyle: "short",
    });
  } catch {
    return iso;
  }
}
