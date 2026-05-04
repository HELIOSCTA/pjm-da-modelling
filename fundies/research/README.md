# PJM Research

One-way snapshot of PJM research notes copied from the obsidian vault
at `helioscta-obsidian/PJM`. Not synced — re-copy to refresh.

**Snapshot date:** 2026-05-01

## Layout

| Path | Contents |
|------|----------|
| `LOGGED/` | Dated briefs (Energy Aspects, vol recaps, PJM events). Primary research log. |
| `TODO/` | Open investigations, one folder per topic (reserves, congestion, weather vs load, ...). |
| `Daily-Prompt/` | Daily-prompt template + prompt iteration log + supporting SQL. |
| `PAPER-TRADING/` | Trade journal, review log/template, trade-log CSV. |
| `@ICE-Trade-Blotter/` | ICE trade blotter screenshots and position-size limits (`MY-LIMITS.md`). |
| `@Reading/` | Source PDFs that the LOGGED briefs are written from. |
| `@Images/` | Image attachments referenced by markdown notes. |
| `PJM-Morning-Fundies.md` | Standing morning fundies note (live in obsidian; this is a snapshot). |
| `TRADING_SIGNALS.md` | Trading-signals reference. |
| `lessons-learned.md` | Running lessons-learned log. |

## Gitignore caveat

The repo `.gitignore` excludes `*.pdf` and `*.csv`. After this snapshot,
the following live on disk but are untracked by git:

- 13 source PDFs in `@Reading/`
- `PAPER-TRADING/trade-log.csv`

The markdown notes (including everything under `TODO/`) are tracked.
If you want the PDFs / CSV checked in too, add a negation rule scoped
to `fundies/research/` in the root `.gitignore`.

## How to update

Re-copy from the obsidian vault (PowerShell):

```powershell
$src = "C:\Users\AidanKeaveny\Documents\github\helioscta-obsidian\PJM"
$dst = "C:\Users\AidanKeaveny\Documents\github\helioscta-pjm-da-data-scrapes\fundies\research"
robocopy $src $dst /E /XD .obsidian .claude /XF .DS_Store "~$*" *.tmp
```

Update the snapshot date at the top of this file when you do.
