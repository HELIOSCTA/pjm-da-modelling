import type { ReactNode } from "react";

// "No forecast run found" panel. Pages compose this beneath their
// PageHeader + (optional) date picker so the empty state still gives
// the user a way to navigate to a date that has data.
export function EmptyStatePanel({
  message,
  hint,
}: {
  message: ReactNode;
  hint?: ReactNode;
}) {
  return (
    <div className="mt-8 rounded border border-gray-800 bg-[#161a23] p-6 text-sm text-gray-300">
      <p>{message}</p>
      {hint ? <p className="mt-2 text-gray-500">{hint}</p> : null}
    </div>
  );
}
