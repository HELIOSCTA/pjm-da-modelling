"""Source -> fleet-schema builders.

Each module reads from one or more ``sources/`` feeds, normalizes to
the unified fleet schema, and writes a parquet artifact. The Excel
builder writes the canonical ``pjm_fleet.parquet`` at the package
root; other builders write parallel parquets under ``artifacts/``.
"""
