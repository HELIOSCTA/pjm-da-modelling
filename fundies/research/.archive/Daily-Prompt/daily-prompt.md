# PJM Daily Morning Fundies — Claude Code Prompt

## How to Run

Invoke with `/pjm-morning-fundies` in Claude Code.

## What It Does

Generates a daily entry in `PJM/PJM-Morning-Fundies.md` by:
1. Pulling ICE short-term power levels from the `pjm-views` MCP server
2. Prompting for Edi's market read

See `.SKILLS/pjm-daily-fundies.md` for the full template and workflow.

## Workflow Diagram

```mermaid
flowchart TD
    START(["/pjm-morning-fundies"]) --> PREREQ{pjm-views MCP\nserver running?}
    PREREQ -- No --> FAIL["Start backend:\nuvicorn src.api.main:app --port 8000"]
    PREREQ -- Yes --> PARALLEL

    subgraph PARALLEL ["Step 1 — Data Pull"]
        direction LR
        MCP1["get_ice_power_intraday\n format=md\n (30-day settle, 3-day intraday)"]
        CTX["Read last 3 entries\nPJM-Morning-Fundies.md"]
    end

    PARALLEL --> EXTRACT

    subgraph EXTRACT ["Step 2 — Extract ICE Levels"]
        direction TB
        PWR["Power table\nBalDay RT / NxtDay DA / NxtDay RT\nBalWeek / Week1"]
        MOVES["Key Moves\n1-2 sentences on what changed"]
    end

    EXTRACT --> EDI

    subgraph EDI ["Step 3 — Edi's Input"]
        direction TB
        Q1["What's Edi's read\non the market today?"]
        Q2["Key levels or setups\nhe's watching?"]
        Q3["Specific trade ideas\ndiscussed?"]
        Q4["Anything else\nworth noting?"]
        Q1 --> Q2 --> Q3 --> Q4
    end

    EDI --> WRITE["Prepend entry\nto PJM-Morning-Fundies.md\n(newest first, never delete)"]
    WRITE --> DONE([Done])

    style START fill:#4a9eff,color:#fff
    style DONE fill:#22c55e,color:#fff
    style FAIL fill:#ef4444,color:#fff
```

## Prerequisites

The `pjm-views` MCP server must be running:
```bash
cd helioscta-pjm-da/backend
uvicorn src.api.main:app --port 8000
```

## Changelog

| Date | Change |
|------|--------|
| 2026-04-13 | **Simplified**: ICE power only (no gas) + Edi chat. Removed trade decision, iteration log, gas table. |
| 2026-04-13 | Format redesign: Stripped 18 SQL queries. ICE views + Edi notes + forced trade decision. |
| 2026-03-13 | Initial 18-query automated prompt with iteration log |
