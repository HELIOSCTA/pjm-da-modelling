# Frontend cron + caching conventions

Standards for adding scheduled work and data endpoints in `frontend/`.
Apply when scaffolding any of:

- a new entry in `frontend/vercel.json` `crons[]`
- a route handler under `frontend/app/api/cron/*`
- a data endpoint that needs to be fast for the browser
- a Postgres-backed snapshot table the frontend reads

The goal is **predictable end-to-end latency for users** by making
data endpoints serve from a pre-warmed Postgres snapshot, not by
querying source marts on the request path.

## Architecture in one paragraph

Vercel Cron hits `/api/cron/<name>` on schedule → the cron handler
authenticates via `Authorization: Bearer ${CRON_SECRET}` → it does the
data work and writes a row into a `*_snapshot` Postgres table with a
`last_refreshed` timestamp. User-facing data endpoints
(`/api/<domain>/...`) read **only** from that snapshot table — they
never touch source marts directly. The cron is the single writer; the
data endpoint is a fast reader. Stale-data behavior is observable in
SQL (`SELECT last_refreshed`), not hidden inside Next's cache layer.

## vercel.json — schedule and function config

Every cron job declares **two** things in `frontend/vercel.json`:
the schedule under `crons[]`, and the per-function timeout/region
under `functions{}`.

```json
{
  "functions": {
    "app/api/cron/warm-da-results/route.ts": {
      "regions": ["sfo1"],
      "maxDuration": 60
    }
  },
  "crons": [
    { "path": "/api/cron/warm-da-results", "schedule": "7,17,27,37,47,57 * * * *" }
  ]
}
```

Conventions:

- **Stagger schedules by minute offset** when multiple warmers run on
  the same cadence. Use a different minute-mod-10 starting point per
  job (`1,11,21,...`, `3,13,23,...`, `5,15,25,...`, `7,17,...`,
  `9,...`). Two warmers on the same minute compete for serverless
  capacity and inflate cold-start tail latency.
- **Pick `maxDuration` deliberately:** 30s for thin wrappers, 60s for
  routine warmers that hit one or two queries, 120-300s for heavy
  ingest. Don't default to 300 — it makes timeouts invisible.
- **Region:** match the Postgres region. Default to `sfo1` to mirror
  the reference repo unless the database is somewhere else.
- **One file = one cron path.** Don't multiplex cron entries into a
  single handler with a `?job=` switch; the schedules diverge over
  time and the function config can't be split.

## Cron handler — the canonical shape

Path: `frontend/app/api/cron/<name>/route.ts`

```ts
import { NextResponse } from "next/server";
import { refreshDaResultsSnapshot } from "@/lib/server/snapshots/da-results";

export const dynamic = "force-dynamic";

function isAuthorized(request: Request): boolean {
  const secret = process.env.CRON_SECRET?.trim();
  if (!secret) return process.env.NODE_ENV !== "production";
  const authHeader = request.headers.get("authorization")?.trim();
  return authHeader === `Bearer ${secret}`;
}

export async function GET(request: Request) {
  if (!isAuthorized(request)) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const startedAt = Date.now();
  try {
    const result = await refreshDaResultsSnapshot();
    return NextResponse.json({
      ok: true,
      rowsWritten: result.rowsWritten,
      sourceQueryMs: result.sourceQueryMs,
      totalElapsedMs: Date.now() - startedAt,
      checkedAt: new Date().toISOString(),
    });
  } catch (error) {
    console.error("[warm-da-results] failed:", error);
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "Unknown error",
        totalElapsedMs: Date.now() - startedAt,
        checkedAt: new Date().toISOString(),
      },
      { status: 500 }
    );
  }
}
```

Required:

- `export const dynamic = "force-dynamic"` — never cache the cron
  response itself.
- `GET` only. Vercel Cron sends GET; don't add POST.
- `isAuthorized` checks the bearer token. In dev (no `CRON_SECRET`
  set), allow through; in prod, reject without the header.
- All real work lives in `lib/server/snapshots/<domain>.ts`, called
  by name. The cron handler is glue + auth + logging.
- Response shape: `{ ok, totalElapsedMs, checkedAt, ...domainTimings }`.
  Skim-readable in Vercel logs.
- Error logging: `console.error("[<name>] ...", err)` with the cron
  name as a bracketed prefix so log searches are trivial.

## Snapshot tables — the data contract

Every warmed dataset has its own `<domain>_snapshot` table in
Postgres. The cron writes; the data endpoint reads. Source marts
are not touched on the request path.

Required columns on every snapshot table:

| Column | Type | Purpose |
|---|---|---|
| `snapshot_id` | bigserial / uuid | row identity |
| `last_refreshed` | timestamptz | when the cron wrote this row |
| `as_of_date` | date | the business date the row describes |
| `payload` or domain columns | jsonb / typed | the actual data |

Conventions:

- **Single-writer:** only one cron job writes to a given snapshot
  table. If two warmers want to write the same table, merge them.
- **Refresh strategy:** `BEGIN; DELETE WHERE as_of_date = $1;
  INSERT ...; COMMIT;` — atomic replace per business date. Don't do
  blind `TRUNCATE` (loses prior days) and don't do `UPSERT` row-by-row
  (slow under load).
- **Read pattern in data endpoints:**
  ```sql
  SELECT payload, last_refreshed
  FROM da_results_snapshot
  WHERE as_of_date = $1
  ORDER BY last_refreshed DESC
  LIMIT 1;
  ```
  The endpoint returns `last_refreshed` in its response so the UI can
  show "as of HH:MM ET" — staleness is a feature, not a bug to hide.
- **Indexes:** at minimum `(as_of_date, last_refreshed DESC)`.
  Add domain-specific indexes for queries the UI actually issues.

The DDL for these tables lives in the Python repo's dbt or migration
folder (TBD on which); the frontend's `lib/server/snapshots/<domain>.ts`
holds the read + refresh queries. Don't duplicate DDL in the frontend.

## Data endpoints — the read path

Path: `frontend/app/api/<domain>/route.ts` (or under a route group).

```ts
import { NextResponse } from "next/server";
import { readDaResultsSnapshot } from "@/lib/server/snapshots/da-results";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const asOfDate = url.searchParams.get("as_of_date") ?? null;

  const snapshot = await readDaResultsSnapshot(asOfDate);
  if (!snapshot) {
    return NextResponse.json({ error: "No snapshot found" }, { status: 404 });
  }

  return NextResponse.json({
    asOfDate: snapshot.as_of_date,
    lastRefreshed: snapshot.last_refreshed,
    data: snapshot.payload,
  });
}
```

Conventions:

- **Read snapshots only.** No source-mart queries in this path. If
  the cron hasn't run yet, return 404 — don't fall back to live
  queries (it makes the slow path invisible).
- `dynamic = "force-dynamic"` so each request reads the freshest
  snapshot row. The Postgres query is fast (single-row index scan);
  Next's HTTP cache adds a layer of confusion we don't need.
- **Always include `lastRefreshed` in the response payload.** The UI
  shows it; observability dashboards key off it.
- **Don't** wrap the read in `unstable_cache` or `cacheLife` — the
  Postgres snapshot already is the cache.

## Auth bypass for cron → data endpoint (deferred)

The reference repo's `middleware.ts` has eight `isTrustedInternal*`
functions that whitelist a (path, tag) pair so the cron's internal
fetch can bypass the NextAuth session redirect. **We don't need this
yet** — we have no auth.

If/when auth lands and a cron needs to call a protected data endpoint
(rather than calling the snapshot module directly), implement the
bypass as **one** function backed by a config map:

```ts
const INTERNAL_CRON_PATHS: Record<string, string> = {
  "/api/da-results": "cron_warm_da_results",
  "/api/transmission-outages": "cron_warm_outages",
};
```

Don't replicate the eight-function pattern from the reference.

Better still: make crons call snapshot modules directly
(`refreshDaResultsSnapshot()`), not HTTP-fetch their own data
endpoint. The HTTP-fetch indirection only existed because the
reference repo's snapshot logic lived inside the data endpoint, not
in a shared module. Our `lib/server/snapshots/*` split removes that
need.

## Logging and observability

- Cron handlers: `console.error("[<cron-name>] ...", err)` for
  errors, `console.log("[<cron-name>] ok in <ms>")` for success only
  if a numeric trace is useful (Vercel logs the JSON response anyway).
- Snapshot modules expose timing in their return value
  (`{ rowsWritten, sourceQueryMs }`) so the cron can surface it in
  the response JSON.
- The UI surfaces `last_refreshed` next to every dashboard. If a
  snapshot is stale, the user sees it — don't silently serve old data
  with a fresh-looking timestamp.

## Anti-patterns (don't)

- **Don't query source marts from a request handler.** If a query
  takes more than 200ms, it belongs behind a snapshot.
- **Don't put data work in the cron handler body.** Snapshot modules
  in `lib/server/snapshots/<domain>.ts` are reusable from scripts and
  tests; route handlers are not.
- **Don't multiplex jobs.** One cron path = one file = one job.
- **Don't default to `maxDuration: 300`.** It hides timeouts that
  would otherwise force you to optimize.
- **Don't fall back to live queries** when a snapshot is missing.
  404 is the right answer; it surfaces the broken cron.
- **Don't rely on Next's `fetch` cache or `unstable_cache`** as the
  primary cache. Postgres snapshots are the cache. Next's cache is
  fine for static asset metadata, not for domain data.
- **Don't write CRON_SECRET checks into middleware.ts** until auth
  lands — Vercel's bearer-token check on the cron handler itself is
  enough.
