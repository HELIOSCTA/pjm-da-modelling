"""Cross-source verification scripts for the canonical fleet parquet.

Each module loads ``pjm_fleet.parquet`` and pulls from one independent
``sources/`` feed, joins, and prints a gap report. Validators never
write to ``pjm_fleet.parquet`` -- only to sidecar ``*_validation.parquet``
files under ``artifacts/``.
"""
