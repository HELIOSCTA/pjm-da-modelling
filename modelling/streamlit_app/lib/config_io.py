"""Save / load named ``KnnModelConfig`` variants as JSON on disk.

``KnnModelConfig`` is the single source of truth for run-level field defaults.
This module:
  - derives ``DEFAULT_PAYLOAD`` from ``KnnModelConfig()`` so adding a field to
    the dataclass automatically propagates here.
  - provides ``payload_to_config`` and ``config_to_payload`` round-trippers.
  - serializes the persistable subset of fields plus a ``per_hour.flt_radius``
    sidecar (which is per-spec, not on the dataclass).
"""
from __future__ import annotations

import dataclasses
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Streamlit pages add the modelling root to sys.path before importing this
# module; if not, fall back to a relative path so unit tests / notebooks work.
_MODELLING_ROOT = Path(__file__).resolve().parents[2]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

from da_models.like_day_model_knn.configs import KnnModelConfig  # noqa: E402

CONFIGS_DIR = Path(__file__).resolve().parents[1] / "saved_configs"

# Run-level fields we persist. Excludes runtime-only fields (forecast_date),
# package-wide constants (hub, schema), and shape-only knobs (quantiles,
# day_type_profiles — overridden in code, not the UI).
PERSISTED_FIELDS: tuple[str, ...] = (
    "n_analogs",
    "season_window_days",
    "min_pool_size",
    "same_dow_group",
    "exclude_holidays",
    "exclude_dates",
    "use_day_type_profiles",
    "max_age_years",
    "recency_half_life_years",
)


def _dataclass_defaults() -> dict[str, Any]:
    cfg = KnnModelConfig()
    return {f: getattr(cfg, f) for f in PERSISTED_FIELDS}


DEFAULT_PAYLOAD: dict[str, Any] = {
    "name": "",
    "description": "",
    **_dataclass_defaults(),
    # flt_radius is per-spec (not on KnnModelConfig); kept as a sidecar.
    "per_hour": {"flt_radius": 1},
}

_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def slugify(name: str) -> str:
    """Sanitize a config name for use as a filename."""
    slug = _NAME_RE.sub("_", name.strip()).strip("._-")
    return slug or "unnamed"


def _path(name: str) -> Path:
    return CONFIGS_DIR / f"{slugify(name)}.json"


def list_configs() -> list[dict[str, Any]]:
    """Return saved configs as dicts, sorted by name."""
    if not CONFIGS_DIR.exists():
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(CONFIGS_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        payload.setdefault("name", path.stem)
        payload["_path"] = str(path)
        out.append(payload)
    return out


def load_config(name: str) -> dict[str, Any] | None:
    path = _path(name)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _opt_int(value: Any) -> int | None:
    if value is None or value == "" or value is False:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _opt_float(value: Any) -> float | None:
    if value is None or value == "" or value is False:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_exclude_dates(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [s.strip() for s in value.splitlines() if s.strip()]
    return [str(s) for s in value]


# Per-field coercion. Each entry maps a payload value to the type
# ``KnnModelConfig`` expects. Keeps payload_to_config tolerant of UI quirks
# (False sliders, "" strings, etc.) without scattering the logic.
_FIELD_COERCERS: dict[str, Any] = {
    "n_analogs": lambda v: int(v),
    "season_window_days": lambda v: int(v),
    "min_pool_size": lambda v: int(v),
    "same_dow_group": lambda v: bool(v),
    "exclude_holidays": lambda v: bool(v),
    "exclude_dates": _normalize_exclude_dates,
    "use_day_type_profiles": lambda v: bool(v),
    "max_age_years": _opt_int,
    "recency_half_life_years": _opt_float,
}


def payload_to_config(
    payload: dict[str, Any] | None,
    *,
    forecast_date: str | None = None,
    model_name: str | None = None,
) -> KnnModelConfig:
    """Build a ``KnnModelConfig`` from a saved payload.

    Missing keys fall through to the dataclass defaults — this is how legacy
    saved configs (predating any new field) keep working without migration.
    """
    if not payload:
        kwargs: dict[str, Any] = {}
    else:
        kwargs = {}
        for f in PERSISTED_FIELDS:
            if f not in payload:
                continue
            coercer = _FIELD_COERCERS.get(f, lambda v: v)
            kwargs[f] = coercer(payload[f])
    if forecast_date is not None:
        kwargs["forecast_date"] = forecast_date
    if model_name is not None:
        kwargs["model_name"] = model_name
    return KnnModelConfig(**kwargs)


def config_to_payload(
    cfg: KnnModelConfig,
    *,
    name: str = "",
    description: str = "",
    flt_radius: int = 1,
) -> dict[str, Any]:
    """Inverse of ``payload_to_config``. Drops dataclass-only fields like
    ``forecast_date`` so the on-disk payload is run-config-only."""
    out: dict[str, Any] = {"name": name, "description": description}
    for f in PERSISTED_FIELDS:
        out[f] = getattr(cfg, f)
    # exclude_dates is mutable; copy so callers can't accidentally mutate the cfg.
    out["exclude_dates"] = list(out["exclude_dates"])
    out["per_hour"] = {"flt_radius": int(flt_radius)}
    return out


def save_config(name: str, payload: dict[str, Any]) -> Path:
    """Persist a payload as JSON. Round-trips through ``KnnModelConfig`` so
    the on-disk record always has canonical types and fills in any missing
    fields with current dataclass defaults."""
    CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    cfg = payload_to_config(payload)
    record = config_to_payload(
        cfg,
        name=name,
        description=str(payload.get("description", "")),
        flt_radius=int(payload.get("per_hour", {}).get("flt_radius", 1)),
    )
    record["updated_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    path = _path(name)
    path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    return path


def delete_config(name: str) -> bool:
    path = _path(name)
    if path.exists():
        path.unlink()
        return True
    return False


def overrides_for(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Return the kwargs to pass to ``single_day.generate()`` for a payload.

    Always includes ``flt_radius`` for downstream callers — non-per_hour
    callers should pop it themselves.
    """
    if not payload:
        return {}
    cfg = payload_to_config(payload)
    out: dict[str, Any] = {f: getattr(cfg, f) for f in PERSISTED_FIELDS}
    out["exclude_dates"] = list(out["exclude_dates"])
    out["flt_radius"] = int(payload.get("per_hour", {}).get("flt_radius", 1))
    return out
