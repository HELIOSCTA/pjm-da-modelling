"""Backtest scoring metrics. Wraps the per-row primitives in
``common.evaluation.metrics`` with frame-level aggregation suited to the
canonical tall schema (one row per target_date x hour_ending x model).
"""
