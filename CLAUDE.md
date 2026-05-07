# helioscta-pjm-da-data-scrapes

PJM day-ahead market modelling and data infrastructure. Active focus:
`modelling/da_models/like_day_model_knn` (KNN like-day analog forecaster).

## Conventions

When writing or substantially modifying a Python script (anything with
a `__main__` block or meant to be run directly), follow
`.claude/standards/python_scripts.md`. Read that file before scaffolding
a new script.

The canonical worked example is
`modelling/da_models/common/data/verify_data_loader.py`.

Cross-family imports flow forward only: every model family
(`like_day_model_knn`, `naive_baselines`, future) may import from
`common/`, but never from a sibling family. If a utility is shared,
lift it to `common/`.

## Layout pointers

- `modelling/da_models/common/` — shared loaders, configs, calendar.
  - `common/data/loader.py` — parquet loaders, single source per
    dataset key in `_DEFAULT_PATTERNS`.
  - `common/data/lmp_pool.py` — DA LMP loading + wide-pivot
    primitives (`build_lmp_labels`, `LMP_HOUR_COLUMNS`).
  - `common/forecast/output.py` — display helpers
    (`actuals_from_pool`, `add_summary_cols`, `build_output_table`).
- `modelling/da_models/like_day_model_knn/` — KNN analog forecaster.
  Variant subpackage: `pjm_rto_hourly/` (long-format pool, one row per
  `(date, hour_ending)`, per-HE scalar matching — sunny-aligned). The
  pre-T4 `flt_radius`-windowed wide pool is gone; `actuals_from_pool`
  and the spec's `feature_groups` reference scalar long col names
  (`load_mw_at_hour`, `solar_at_hour`, `lmp`, etc., catalogued in
  `domains.HOURLY_STEM_TO_LONG_COL`).
- `modelling/da_models/naive_baselines/` — literature-canonical
  reference forecasters (EPF naive per Lago/Nogales/Conejo, pure d-7).
  Used as the rMAE denominator for richer models.
- `modelling/data/cache/` — parquet cache.
- `modelling/streamlit_app/` — operator console.

## Data loader conventions

When pulling **load + solar + wind + net_load together** for the same
`(region, date)`, use the unified loaders:

- `loader.load_pjm_supply_demand_coalesced(region="RTO")` — PJM forecast
  with PJM RT fallback. RTO only.
- `loader.load_meteologica_supply_demand_coalesced()` — Meteologica
  forecast with PJM RT fallback. 4 regions.

Both make a **single forecast-vs-RT decision per `(region, date)` for all
four series**, so the identity `net_load = load - solar - wind` holds
within each row by construction.

The per-series coalescers (`load_load_coalesced`, `load_solar_coalesced`,
`load_wind_coalesced`, `load_pjm_net_load_coalesced`, and the Meteologica
equivalents) remain valid for **single-series consumption** or
**intentional per-series display-comparison** (streamlit Data page,
individual check_loaders). **Never compose `load - solar - wind` from
their outputs** — each decides forecast-vs-RT independently and the
identity breaks on dates with mixed coverage (concrete repro:
2025-05-01, ~3.9 GW max gap).

**Forecast-vs-RT rule.** Across all coalesced loaders: the DA-cutoff
forecast wins when the historical mart has all 24 `hour_ending` values
present; RT actuals fill every other `(region, date)`. `lead_days=1` is
the DA-cutoff vintage default (`as_of_date == forecast_date - 1`); pass
`lead_days=None` to skip the vintage filter.

**Region scope.** Source coverage by region:

| Series | Forecast | RT actuals |
|---|---|---|
| load | RTO + MIDATL/WEST/SOUTH | RTO + MIDATL/WEST/SOUTH |
| solar | system-wide (treated RTO) | RTO + MIDATL/WEST/SOUTH |
| wind | system-wide (treated RTO) | RTO + MIDATL/WEST/SOUTH |
| net_load (PJM) | RTO only | RTO + MIDATL/WEST/SOUTH |
| net_load (Meteologica) | RTO + MIDATL/WEST/SOUTH | RTO + MIDATL/WEST/SOUTH |

Therefore `load_pjm_supply_demand_coalesced` is RTO-only by design — PJM
solar/wind/net_load forecasts don't exist sub-zonally. Sub-zonal demand
needs Meteologica (`load_meteologica_supply_demand_coalesced`), or the
per-series load loader if only load is needed.

## Frontend (`frontend/`)

Next.js 15 App Router + React 19 + TypeScript + Tailwind 3, deployed
to Vercel with `frontend/` as the project root. The Python repo and
the frontend share git history but are independent build targets —
nothing in `frontend/` imports Python or reads the modelling tree
from disk. Data flows through Postgres (Azure-hosted marts and model
outputs) and Azure Blob (larger artifacts), never the filesystem.

### Layout pointers

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

### Conventions

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
  `frontend/app/layout.tsx`; see the Styling standards section
  below before changing colors or fonts.
- **Linting.** `npm run lint` (default `eslint-config-next`) must
  pass before commit. Don't loosen the ruleset without a written
  reason.

### Styling standards

The frontend ships a single dark theme. Light mode is not on the
roadmap — operator-facing dashboards run all day next to terminal
windows and chart tools, so the dark surface is the design intent,
not a preference toggle.

Tokens, defined once in `frontend/app/globals.css` and exposed to
Tailwind in `frontend/tailwind.config.ts`:

- `--background: #0f1117` — page background. Tailwind: `bg-background`.
- `--foreground: #e5e7eb` — primary text. Tailwind: `text-foreground`.
- Body font stack: system (`-apple-system, BlinkMacSystemFont,
  "Segoe UI", Roboto, sans-serif`) — no custom web fonts.

When extending the palette:

- Prefer Tailwind's built-in slate / gray scale for surfaces
  (`bg-gray-900/60`, `border-gray-800`, `text-slate-400`) — that's
  what the reference app uses and what reads correctly against
  `#0f1117`. Don't introduce a parallel "neutral-X" or "zinc-X"
  scale; pick one ramp and stay on it.
- Accent colors (current-period highlight, error, baseline-fit, etc.)
  are codified in chart-token files, not redefined per component.
  When charts arrive, add `frontend/lib/chartConstants.ts` mirroring
  the spark-spread-viz file (slate gridlines, cyan current-period,
  red error, amber baseline-fit) and import from there — no
  per-chart hex literals.
- Avoid `bg-white` / `text-black` anywhere in the app. If a surface
  needs to pop against the page background, layer a darker
  translucent panel (`bg-gray-900/60`, `border-gray-800`) rather
  than inverting to a light surface.
- Charts and tables: ticks `#94a3b8`, gridlines `#1f2937`, zero/axis
  baseline `#475569`. Keep these in `lib/chartConstants.ts` once
  it exists.

Hydration: the body sets `suppressHydrationWarning` because browser
extensions inject attributes that trip Next.js's mismatch warning on
the dark background. Don't remove it.

### Cron + caching

When scaffolding a `frontend/app/api/cron/*` handler, a new entry in
`frontend/vercel.json`, or any data endpoint that needs to be fast,
follow `.claude/standards/frontend_cron_caching.md`. Read that file
before writing the code.

Headline rules from that doc:

- Data endpoints read from a Postgres `*_snapshot` table; cron jobs
  write to it. Source marts are never queried on the request path.
- One cron path = one file = one job. Schedules are staggered by
  minute offset so warmers don't pile up.
- Cron handlers are thin — auth + call a `lib/server/snapshots/*`
  module + return timing JSON. Real work lives in the snapshot module.
- Every data endpoint response includes `lastRefreshed` so staleness
  is observable in the UI.

### Vercel project setup

- **Root Directory:** `frontend` (set in the Vercel dashboard so the
  Python tree is excluded from builds).
- **Cron jobs / function regions / max durations:** declared in
  `frontend/vercel.json` once endpoints exist. No `vercel.json` is
  required for the blank-page deploy.
- **Environment variables:** managed in the Vercel dashboard, scoped
  per environment. Mirror locally via `frontend/.env.local`
  (gitignored).

## CLAUDE.md / MEMORY.md / settings.json — routing rule

When recording a fact, route by **scope**:

- **Personal preference** (terseness, individual style, your past
  corrections) → `MEMORY.md`. Per-user, not checked in. Auto-maintained.
- **Project / team-visible convention** (this file's territory: repo
  layout, naming conventions, where shared utilities live, standards
  docs) → propose an edit to `CLAUDE.md` before relying on memory
  alone. End-of-task: if conventions established this session aren't
  documented here, surface them.
- **Mechanical rule that must run every tool call** (formatting,
  lint, no-emoji enforcement) → `.claude/settings.json` hook.

Same fact rarely belongs in two places. When in doubt, prefer
`CLAUDE.md` for repo-wide truths and `MEMORY.md` for the user's
voice.
