"""Shared HTML render entrypoint for backend report orchestrators.

Each top-level report (da_report.py, morning_report.py, ...) composes
`build_fragments()` outputs from one or more bundles and passes the
combined list to `render()`. Replaces the per-bundle pipeline.py.
"""
from __future__ import annotations

from backend.utils import paths
from backend.utils.html_dashboard import HTMLDashboardBuilder

Fragment = tuple[str, str, str | None]


def render(
    *,
    title: str,
    output_name: str,
    sections: list[str | Fragment],
    theme: str = "dark",
) -> str:
    builder = HTMLDashboardBuilder(title=title, theme=theme)
    for item in sections:
        if isinstance(item, str):
            builder.add_divider(item)
        else:
            name, html, icon = item
            builder.add_content(name, html, icon=icon)

    out = paths.OUTPUT_DIR / output_name
    out.parent.mkdir(parents=True, exist_ok=True)
    builder.save(str(out))
    return str(out)
