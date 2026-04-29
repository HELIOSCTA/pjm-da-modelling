# mcp_server

FastAPI + MCP entry point for serving structured view models to agents and frontends.
First slice: a single endpoint exposing PJM transmission outages.

## Run

From the repo root:

```bash
uvicorn backend.mcp_server.main:app --reload
```

Endpoint: `GET /views/transmission_outages?format=md|json`
MCP transport is mounted via `FastApiMCP(app).mount_http()` at `/mcp`.

## Required environment variables

Loaded from `backend/.env` by `backend/credentials.py`:

- `AZURE_POSTGRESQL_DB_HOST`
- `AZURE_POSTGRESQL_DB_USER`
- `AZURE_POSTGRESQL_DB_PASSWORD`
- `AZURE_POSTGRESQL_DB_PORT`
- `AZURE_POSTGRESQL_DB_NAME`

Non-secret config comes from `backend/settings.py` (no extra vars required for this endpoint).

## Layout

```
backend/mcp_server/
├── main.py                          # FastAPI app + transmission_outages route + MCP mount
├── data/
│   ├── transmission_outages.py      # pull() — calls backend.utils.azure_postgresql_utils.pull_from_db
│   ├── sql_templates.py             # render_sql_template helper
│   └── sql/
│       └── pjm_transmission_outages.sql
└── views/
    ├── transmission_outages.py      # build_view_model()
    └── markdown_formatters.py       # format_transmission_outages()
```

## Reused infra (not re-ported)

- `backend.utils.azure_postgresql_utils.pull_from_db` — Postgres pull
- `backend.settings` / `backend.credentials` — env loading
