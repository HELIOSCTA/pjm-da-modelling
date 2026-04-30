# Python script conventions

Standards for runnable Python scripts in this repo (e.g.
`modelling/da_models/common/data/verify_data_loader.py`). Apply when
writing or substantially modifying any module with a `__main__` block
or any script meant to be run directly.

## Entry point and arguments

- **No `argparse` for routine scripts.** Define tunable defaults as
  module-level constants near the top of the file:
  ```python
  REGION: str = "RTO"
  CACHE_DIR: Path | None = None
  ```
  Change them by editing the file or by calling `run(...)` from a
  notebook. Reserve `argparse` only for scripts that genuinely take
  user input on every run.
- Single entry point named `run(...)`. Helper functions accept the
  same defaults so they're independently callable from notebooks.
- The `__main__` block is a one-liner:
  ```python
  if __name__ == "__main__":
      run()
  ```

## Imports

- `from __future__ import annotations` first.
- Order: stdlib → third-party → local (`from da_models...`).
- Local imports go AFTER the `sys.path` bootstrap (annotate them
  `# noqa: E402`) when the script lives deep in the package tree.

## Path bootstrap

Scripts that import sibling packages need explicit `sys.path` adjustment
so they run both as `python -m pkg.mod` and `python path/to/mod.py`:

```python
_MODELLING_ROOT = Path(__file__).resolve().parents[N]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))
```

`parents[N]` depth depends on where the file lives. Verify by running
both forms once.

## Console output

- Reconfigure stdout / stderr to UTF-8 at the top of `run()`:
  ```python
  for stream in (sys.stdout, sys.stderr):
      reconfigure = getattr(stream, "reconfigure", None)
      if callable(reconfigure):
          reconfigure(encoding="utf-8", errors="replace")
  ```
- **ASCII-only output.** No emojis, no Unicode box-drawing characters
  in printed strings — they raise `UnicodeEncodeError` on the Windows
  console (cp1252) without `PYTHONIOENCODING=utf-8` set. Use `===` /
  `---` / `|` style separators.
- For tables, prefer `f"{value:>10,.1f}"`-style format specifiers and
  `pd.DataFrame.to_string(index=False)` over external tabulate libs.

## Type hints

All public functions get type hints. Use modern syntax (`Path | None`,
not `Optional[Path]`); Python 3.10+ is the floor here.

## Logging vs print

- Ad-hoc verification / one-shot diagnostic scripts: plain `print()`.
- Pipeline / orchestration / production code: `logging` module.

## Reference template

```python
"""One-line summary.

Longer doc explaining what the script does, where its defaults live,
and how to invoke it (notebook + CLI).

Usage::

    python -m da_models.<pkg>.<mod>
"""
from __future__ import annotations

import sys
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[N]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402

from da_models.common.data import loader  # noqa: E402

# ── Defaults (edit here instead of using CLI flags) ────────────────
DEFAULT_X: str = "..."
CACHE_DIR: Path | None = None


def helper(x: str = DEFAULT_X, cache_dir: Path | None = CACHE_DIR) -> ...:
    ...


def run(x: str = DEFAULT_X, cache_dir: Path | None = CACHE_DIR) -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")
    result = helper(x=x, cache_dir=cache_dir)
    # report ...


if __name__ == "__main__":
    run()
```

## Working example in repo

`modelling/da_models/common/data/verify_data_loader.py` follows this
standard end-to-end. Use it as the canonical example when scaffolding a
new script.
