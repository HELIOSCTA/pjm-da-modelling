"""Upstream feed readers for the supply-stack fleet pipeline.

Each module exposes ``pull_*`` functions that return raw DataFrames
from one upstream source -- no fleet-schema knowledge here. Consumers
in ``builders/`` and ``validators/`` import from this package.
"""
