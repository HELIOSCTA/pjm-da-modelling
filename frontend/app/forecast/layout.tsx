import { headers } from "next/headers";

import { TabBar } from "./_components/TabBar";
import { DEFAULT_TAB, TAB_DEFS, type TabKey } from "./_lib/tabs";

function tabFromPath(pathname: string): TabKey {
  for (const tab of TAB_DEFS) {
    if (pathname === tab.href || pathname.startsWith(tab.href + "/")) {
      return tab.key;
    }
  }
  return DEFAULT_TAB;
}

export default async function ForecastLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  // next/navigation usePathname is client-only; in a server layout we
  // pull the path off the request headers (Next.js exposes the URL via
  // x-invoke-path / referer / next-url depending on rendering mode).
  // The layout is wrapped on every nested route, so this fires for
  // each render.
  const h = await headers();
  const pathname =
    h.get("x-invoke-path") ??
    h.get("next-url") ??
    h.get("x-pathname") ??
    "/forecast";
  const active = tabFromPath(pathname);

  return (
    <>
      <TabBar active={active} />
      {children}
    </>
  );
}
