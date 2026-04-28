"""Named feature-weight configs for the daily scoreboard.

Each entry is a zero-arg factory returning a fresh ForwardOnlyKNNConfig so
mutations during a run cannot leak between configs.

The baseline weights are pinned here explicitly. configs.FEATURE_GROUP_WEIGHTS
can drift without invalidating the historical interpretation of the
"baseline" row in the scoreboard.
"""
from __future__ import annotations

from typing import Callable

from da_models.forward_only_knn.configs import ForwardOnlyKNNConfig

BASELINE_WEIGHTS: dict[str, float] = {
    "load_level":      3.0,
    "load_ramps":      1.0,
    "gas_level":       2.0,
    "outage_level":    2.0,
    "renewable_level": 1.5,
    "net_load":        2.0,
    "calendar_dow":    1.0,
}


def _with(**overrides: float) -> dict[str, float]:
    out = dict(BASELINE_WEIGHTS)
    out.update(overrides)
    return out


def _baseline() -> ForwardOnlyKNNConfig:
    return ForwardOnlyKNNConfig(feature_group_weights=dict(BASELINE_WEIGHTS))


def _netload_3x() -> ForwardOnlyKNNConfig:
    return ForwardOnlyKNNConfig(feature_group_weights=_with(net_load=3.0))


def _netload_4x_renewables_3x() -> ForwardOnlyKNNConfig:
    return ForwardOnlyKNNConfig(
        feature_group_weights=_with(net_load=4.0, renewable_level=3.0),
    )


def _low_load_low_gas() -> ForwardOnlyKNNConfig:
    return ForwardOnlyKNNConfig(
        feature_group_weights=_with(load_level=2.0, gas_level=1.0),
    )


def _tight_system() -> ForwardOnlyKNNConfig:
    return ForwardOnlyKNNConfig(
        feature_group_weights=_with(
            load_level=2.0,
            gas_level=1.0,
            renewable_level=3.0,
            net_load=4.0,
        ),
    )


def _outage_regime_off() -> ForwardOnlyKNNConfig:
    """Baseline weights but disables the outage regime z-score filter (control)."""
    return ForwardOnlyKNNConfig(
        feature_group_weights=dict(BASELINE_WEIGHTS),
        apply_outage_regime_filter=False,
    )


CONFIG_REGISTRY: dict[str, Callable[[], ForwardOnlyKNNConfig]] = {
    "baseline":                  _baseline,
    "netload_3x":                _netload_3x,
    "netload_4x_renewables_3x":  _netload_4x_renewables_3x,
    "low_load_low_gas":          _low_load_low_gas,
    "tight_system":              _tight_system,
    "outage_regime_off":         _outage_regime_off,
}
