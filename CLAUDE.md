# helioscta-pjm-da-data-scrapes

PJM day-ahead market modelling and data infrastructure. Active focus:
`modelling/da_models/like_day_model_knn` (KNN like-day analog forecaster).

## Top-level layout

- `modelling/` — Python forecasters, data loaders, the streamlit
  operator console. See `modelling/CLAUDE.md` for loader conventions
  and the family-import rule.
- `frontend/` — Next.js 15 / React 19 dashboard deployed to Vercel,
  reads Postgres marts directly. See `frontend/CLAUDE.md` for layout,
  conventions, and styling/cron skills.
- `backend/` — dbt project (`backend/dbt/`), MCP server
  (`backend/mcp_server/`), Prefect orchestration, and shared cache.
- `fundies/` — fundamentals research notes (markdown).
- `azure-infra/` — provisioning shell scripts.
- `scratch/` — throwaway diagnostics, not on the production path.

## Cross-subtree contracts

These are the seams where one subtree writes and another reads.
Knowing them up-front prevents the "I added a feature in modelling/
but the frontend can't see it" class of bug.

- **Forecast runs → `pjm_model_outputs.forecast_runs`.** Every
  Python forecaster publishes one row per run as a single jsonb
  `payload`. The frontend reads exclusively from this table — there
  is no blob/file hop. PK is `(model_name, target_date, run_id)`;
  `model_family` lets the frontend group runs for tabs/pickers,
  `target_date` is the delivery date and `run_date` the forecast
  vintage (`target_date - run_date` = lead days). Latest-for-(model,
  date) is derived (`ORDER BY run_date DESC, created_at DESC LIMIT
  1`), no separate pointer row.
  - **Writers**: each `publish.py` owns its family's `build_payload` /
    `extract_onpeak_forecast`; the upsert itself is the single
    `publish_forecast_run` in `modelling/da_models/common/publish.py`,
    which delegates DDL + write to `utils.azure_postgresql_utils`
    (creates the schema/table on first run). Pipelines compose
    `build_payload -> extract_onpeak_forecast -> publish_forecast_run`.
    New forecasters add a `publish.py` next to their pipeline.
  - **Readers**: `frontend/lib/server/forecastRuns.ts`
    (`listForecastRuns`, `readForecastRun`,
    `readLatestForecastRun`). Frontend tabs follow the canonical
    layout in the `forecast-tab-shell` skill.
  - **Schema**: see `modelling/da_models/common/publish.py` for the
    column list. Currently 9 columns: model_family, model_name,
    run_date, target_date, da_lmp_total_onpeak_forecast, payload
    (jsonb), run_id, plus created_at / updated_at audit columns added
    by the upsert helper. Run-creation timestamps (created_at_utc /
    created_at_local) live inside the payload jsonb.

## CLAUDE.md / MEMORY.md / skills / settings.json — routing rule

When recording a fact, route by **scope**:

- **Personal preference** (terseness, individual style, your past
  corrections) → `MEMORY.md`. Per-user, not checked in. Auto-maintained.
- **Repo-wide truth** (top-level layout, the routing rule itself) →
  this file. Always loaded.
- **Subtree-specific convention** (Python loader rules, frontend
  layout) → a nested `<subtree>/CLAUDE.md`. Loads only when working
  in that subtree.
- **Conditionally-relevant standards** (apply only when doing X kind
  of work — scaffolding a Python script, writing a cron handler,
  styling a component) → a skill under `.claude/skills/<name>/SKILL.md`.
  Skill bodies load on-demand.
- **Mechanical rule that must run every tool call** (formatting,
  lint, no-emoji enforcement) → `.claude/settings.json` hook.

Same fact rarely belongs in two places. When in doubt: `CLAUDE.md`
for repo-wide truths, nested CLAUDE.md for subtree conventions, a
skill for "only when doing X", `MEMORY.md` for the user's voice.

## Compact instructions

When the conversation is auto-compacted, preserve: the top-level
layout above, the routing rule, the active subtree's CLAUDE.md
content (modelling or frontend), and the current task's working
context. Tool outputs and exploratory file reads can be summarized.
