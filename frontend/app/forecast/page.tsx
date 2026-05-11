import { redirect } from "next/navigation";

import { TAB_DEFS, DEFAULT_TAB } from "./_lib/tabs";

// /forecast → bounce to the default tab, preserving query params so
// shared links like /forecast?target_date=2026-05-08 still land
// somewhere meaningful.
export default async function ForecastIndexPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const defaultHref = TAB_DEFS.find((t) => t.key === DEFAULT_TAB)!.href;
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (typeof v === "string" && v.length > 0) qs.set(k, v);
  }
  const target = qs.toString() ? `${defaultHref}?${qs.toString()}` : defaultHref;
  redirect(target);
}
