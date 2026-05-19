# artifacts/

Secondary parquet outputs of the supply-stack fleet pipeline. The
canonical fleet artifact (`pjm_fleet.parquet`) lives one level up
because `data/fleet.py` reads it directly; everything here is either a
parallel fleet build or a validator sidecar.

## What writes what

| File | Writer | Notes |
|---|---|---|
| `pjm_fleet_pudl.parquet` | `builders/build_from_pudl.py` | Per-unit fleet from PUDL EIA-860/923 (`out_eia__monthly_generators` + `core_eia860__scd_plants`). Parallel to the Excel-built `pjm_fleet.parquet`. |
| `pudl_generators_audit.parquet` | `builders/build_from_pudl.py` | Pre-aggregation generator-level audit trail from PUDL: `plant_id_eia`, `generator_id`, EIA fields. Used by CEMS/EIA-923 validators for join keys. |
| `gas_cems_validation.parquet` | `validators/verify_gas_vs_cems.py` | Per-plant annual cap factor + implied heat rate from EPA CEMS. |
| `nuclear_nrc_validation.parquet` | `validators/verify_nuclear_vs_nrc.py` | Per-unit current-day + 30d/365d power utilization from NRC. |

## What does NOT live here

- `pjm_fleet.parquet` — the canonical runtime artifact, one level up.
- PSS/E network artefacts — those live in `backend/mcp_server/data/`,
  not this fleet tree.
