"""Save / load named KnnModelConfig variants as JSON on disk."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CONFIGS_DIR = Path(__file__).resolve().parents[1] / "saved_configs"

DEFAULT_PAYLOAD: dict[str, Any] = {
    "name": "",
    "description": "",
    "n_analogs": 20,
    "season_window_days": 60,
    "min_pool_size": 100,
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


def save_config(name: str, payload: dict[str, Any]) -> Path:
    CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "name": name,
        "description": payload.get("description", ""),
        "n_analogs": int(payload["n_analogs"]),
        "season_window_days": int(payload["season_window_days"]),
        "min_pool_size": int(payload["min_pool_size"]),
        "per_hour": {
            "flt_radius": int(payload.get("per_hour", {}).get("flt_radius", 1)),
        },
        "updated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
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
    """Return the kwargs to pass to single_day.generate() for a config payload.

    Only emits keys that single_day.generate() accepts; passes integers, not
    strings. ``flt_radius`` is included even for non-per_hour models — callers
    must pop it themselves before calling per_day_* generate().
    """
    if not payload:
        return {}
    return {
        "n_analogs": int(payload["n_analogs"]),
        "season_window_days": int(payload["season_window_days"]),
        "min_pool_size": int(payload["min_pool_size"]),
        "flt_radius": int(payload.get("per_hour", {}).get("flt_radius", 1)),
    }
