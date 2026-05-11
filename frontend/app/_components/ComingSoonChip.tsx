export function ComingSoonChip({ label = "soon" }: { label?: string }) {
  return (
    <span className="rounded bg-gray-800/60 px-1.5 py-0.5 text-[9px] uppercase tracking-wider text-gray-500">
      {label}
    </span>
  );
}
