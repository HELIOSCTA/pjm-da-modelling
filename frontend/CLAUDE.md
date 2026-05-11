# frontend/

Next.js 15 App Router + React 19 + TypeScript + Tailwind 3, deployed
to Vercel with `frontend/` as the project root. The Python repo and
the frontend share git history but are independent build targets —
nothing in `frontend/` imports Python or reads the modelling tree
from disk. Data flows through Postgres (Azure-hosted marts and model
outputs) and Azure Blob (larger artifacts), never the filesystem.

## Scope

v1 nav advertises one section: **DA Forecast** (`/forecast` — Like-Day
KNN, ICE-Anchored Meteo, and the model-vs-model Compare tab, all reading
`pjm_model_outputs.forecast_runs`). The ICE Pricing route (`/ice-pricing`)
and future sections still resolve by URL; they're just not in the sidebar
or the home grid yet. Re-adding a "soon" entry is a one-line change in
`app/_components/Sidebar.tsx`.

## Layout pointers

- `frontend/app/` — App Router routes, layouts, server components.
- `frontend/app/api/` — route handlers (REST endpoints, cron targets).
- `frontend/app/api/cron/*` — scheduled warmers, wired through
  `frontend/vercel.json` once endpoints exist.
- `frontend/components/` — shared React components. Co-locate
  route-specific components under their route directory instead.
- `frontend/lib/` — isomorphic, non-React utilities (formatters,
  domain helpers, types).
- `frontend/lib/server/` — server-only modules (DB clients, secrets
  consumers). Each file in this directory must `import "server-only"`
  at the top so accidental client imports fail loudly at build time.
- `frontend/types/` — shared TypeScript types not tied to a route.

## Conventions

- **Server vs client.** Default to Server Components. Add
  `"use client"` only when the component needs hooks, browser APIs,
  or event handlers.
- **Data fetching.** Server Components and route handlers query
  Postgres directly via `pg` (or `@neondatabase/serverless`) — no
  separate Python API layer. Client-side `useEffect` is for
  interactivity, not initial data.
- **Caching.** Lean on Next.js `fetch` cache + `revalidateTag` /
  `revalidatePath`. Cron warmers under `app/api/cron/*` are the
  canonical refresh path.
- **Secrets.** Server-only env vars must not be `NEXT_PUBLIC_*`
  prefixed. DB connection strings, API keys, blob SAS tokens — all
  consumed inside `lib/server/`.
- **Styling.** Tailwind utility classes only — no CSS modules or
  styled-components. Reach for `clsx` / `cva` if class composition
  gets unwieldy. Theme is set in `frontend/app/globals.css` +
  `frontend/app/layout.tsx`. For colors, fonts, palette extension,
  and chart tokens, use the `frontend-styling` skill.
- **Linting.** `npm run lint` (default `eslint-config-next`) must
  pass before commit. Don't loosen the ruleset without a written
  reason.
- **Cron + caching.** When scaffolding `frontend/app/api/cron/*`
  handlers, a new entry in `frontend/vercel.json`, or any data
  endpoint that needs to be fast, use the `frontend-cron-caching`
  skill.
- **Vercel project setup.** Root Directory, env vars, and
  deploy-time config are covered in the `vercel-project-setup`
  skill.
- **Verifying changes.** Any edit that affects what the browser
  renders — components, route handlers, type changes, blob/DB
  readers, chart series, table rows — must close with the
  `frontend-verify` skill probe before reporting done. API-level
  evidence is necessary but not sufficient; fetch the rendered
  page HTML and grep for the user-visible signal.
