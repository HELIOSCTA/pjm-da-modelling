"""Single Scrollable HTML Dashboard Builder.

Ported from helioscta_python (helioscta.utils.html_utils_single_dashboard).
All content on one page; navigation jumps to sections via anchor links.
"""
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

THEME_LIGHT = {
    'body_bg': '#ffffff',
    'body_text': '#333333',
    'header_bg': '#ffffff',
    'header_title_color': '#0078d4',
    'header_border': '#e0e0e0',
    'menu_toggle_color': '#333333',
    'menu_toggle_hover_bg': '#f0f0f0',
    'timestamp_bg': '#f0f0f0',
    'timestamp_text': '#666666',
    'sidebar_bg': '#ffffff',
    'sidebar_border': '#e0e0e0',
    'nav_color': '#0078d4',
    'nav_hover_bg': '#f5f5f5',
    'nav_active_bg': '#e8f4f8',
    'nav_active_color': '#0078d4',
    'nav_active_border': '#0078d4',
    'nav_divider_border': '#e0e0e0',
    'nav_divider_label_color': '#666666',
    'outer_sidebar_bg': '#f4f4f4',
    'outer_sidebar_border': '#dcdcdc',
    'main_content_bg': '#ffffff',
    'section_border': '#e0e0e0',
    'section_header_bg': '#ffffff',
    'section_header_color': '#333333',
    'section_header_border': '#333333',
    'table_container_border': '#dddddd',
    'search_bg': '#ffffff',
    'search_border': '#dddddd',
    'search_text': '#333333',
    'table_th_bg': '#0078d4',
    'table_th_color': '#ffffff',
    'table_td_border': '#eeeeee',
    'table_even_bg': '#fafafa',
    'table_odd_bg': '#ffffff',
    'table_hover_bg': '#f0f0f0',
    'scrollbar_track': '#f1f1f1',
    'scrollbar_thumb': '#cccccc',
    'scrollbar_thumb_hover': '#aaaaaa',
}

THEME_DARK = {
    'body_bg': 'radial-gradient(1200px circle at 18% -8%, #233a5a 0%, #0b1220 42%, #070b14 100%)',
    'body_text': '#dbe7ff',
    'header_bg': 'rgba(8, 14, 24, 0.88)',
    'header_title_color': '#8dd9ff',
    'header_border': '#2a3f60',
    'menu_toggle_color': '#c5d8f2',
    'menu_toggle_hover_bg': '#1a2a42',
    'timestamp_bg': '#132138',
    'timestamp_text': '#a6bad6',
    'sidebar_bg': '#0f1a2b',
    'sidebar_border': '#253b59',
    'nav_color': '#9eb4d3',
    'nav_hover_bg': '#1a2b44',
    'nav_active_bg': '#20314d',
    'nav_active_color': '#dbe7ff',
    'nav_active_border': '#4cc9f0',
    'nav_divider_border': '#243a57',
    'nav_divider_label_color': '#6f8db1',
    'outer_sidebar_bg': '#0a1422',
    'outer_sidebar_border': '#1c2e48',
    'main_content_bg': 'transparent',
    'section_border': '#20324c',
    'section_header_bg': '#111d31',
    'section_header_color': '#dbe7ff',
    'section_header_border': '#4cc9f0',
    'table_container_border': '#2a3f60',
    'search_bg': '#101d31',
    'search_border': '#2a3f60',
    'search_text': '#dbe7ff',
    'table_th_bg': '#16263d',
    'table_th_color': '#e6efff',
    'table_td_border': '#243853',
    'table_even_bg': '#0f1a2b',
    'table_odd_bg': '#132138',
    'table_hover_bg': '#1c2f4a',
    'scrollbar_track': '#0f1a2b',
    'scrollbar_thumb': '#335074',
    'scrollbar_thumb_hover': '#4b6e96',
}


class HTMLDashboardBuilder:
    """Builder for single scrollable HTML dashboards with anchor navigation."""

    def __init__(
        self,
        title: str = "Dashboard",
        theme: str = "dark",
        sidebar_width: int = 220,
        show_header: bool = True,
        full_width: bool = True,
        header_bg_color: str = None,
        header_text_color: str = None,
        primary_color: str = None,
        sidebar_bg_color: str = None,
        nav_font_color: str = None,
    ):
        self.title = title
        self.sidebar_width = sidebar_width
        self.show_header = show_header
        self.full_width = full_width

        self.colors = dict(THEME_DARK if theme == "dark" else THEME_LIGHT)
        if header_bg_color:
            self.colors['header_bg'] = header_bg_color
        if header_text_color:
            self.colors['header_title_color'] = header_text_color
        if primary_color:
            self.colors['table_th_bg'] = primary_color
        if sidebar_bg_color:
            self.colors['sidebar_bg'] = sidebar_bg_color
        if nav_font_color:
            self.colors['nav_color'] = nav_font_color
            self.colors['nav_active_color'] = nav_font_color
            self.colors['nav_active_border'] = nav_font_color

        self.nav_items: List[Tuple[str, str, Optional[str]]] = []

    def add_content(
        self,
        name: str,
        content: Any,
        icon: Optional[str] = None,
        **kwargs,
    ) -> "HTMLDashboardBuilder":
        html_content = self._convert_to_html(content, **kwargs)
        self.nav_items.append((name, html_content, icon))
        return self

    def add_group(
        self,
        items_dict: Dict[str, Any],
        icon: Optional[str] = None,
        **kwargs,
    ) -> "HTMLDashboardBuilder":
        for name, content in items_dict.items():
            self.add_content(name, content, icon=icon, **kwargs)
        return self

    def add_divider(self, label: Optional[str] = None) -> "HTMLDashboardBuilder":
        self.nav_items.append((f"__DIVIDER__{label or ''}", "__DIVIDER__", None))
        return self

    def _convert_to_html(self, content: Any, **kwargs) -> str:
        if isinstance(content, str):
            return content
        elif isinstance(content, pd.DataFrame):
            return self._dataframe_to_html(content, **kwargs)
        elif hasattr(content, 'to_html'):
            return content.to_html(include_plotlyjs='cdn', full_html=False)
        elif hasattr(content, 'to_plotly_json'):
            return self._plotly_to_html(content)
        else:
            return f"<div><pre>{str(content)}</pre></div>"

    def _dataframe_to_html(
        self,
        df: pd.DataFrame,
        height: int = 500,
        **kwargs,
    ) -> str:
        table_id = f"table_{id(df)}"
        data_json = df.to_json(orient='records')
        columns = list(df.columns)

        html = f"""
        <div class="table-container" style="height: {height}px; overflow: auto;">
            <input type="text" id="{table_id}_search" placeholder="Search..." class="search-input">
            <table id="{table_id}" class="data-table">
                <thead>
                    <tr>
                        {''.join(f'<th>{col}</th>' for col in columns)}
                    </tr>
                </thead>
                <tbody id="{table_id}_body"></tbody>
            </table>
        </div>
        <script>
        (function() {{
            const data = {data_json};
            const tableBody = document.getElementById('{table_id}_body');
            const searchInput = document.getElementById('{table_id}_search');

            function renderTable(filteredData) {{
                tableBody.innerHTML = filteredData.map((row, idx) =>
                    `<tr class="${{idx % 2 === 0 ? 'even' : 'odd'}}">
                        {' '.join(f"<td>${{row['{col}'] ?? ''}}</td>" for col in columns)}
                    </tr>`
                ).join('');
            }}

            searchInput.addEventListener('input', function(e) {{
                const searchTerm = e.target.value.toLowerCase();
                const filtered = data.filter(row =>
                    Object.values(row).some(val => String(val).toLowerCase().includes(searchTerm))
                );
                renderTable(filtered);
            }});

            renderTable(data);
        }})();
        </script>
        """
        return html

    def _plotly_to_html(self, fig) -> str:
        import plotly.graph_objects as go
        if isinstance(fig, go.Figure):
            return fig.to_html(include_plotlyjs='cdn', full_html=False)
        return str(fig)

    def _generate_section_id(self, name: str, idx: int) -> str:
        safe_id = "".join(c if c.isalnum() else "_" for c in name.lower())
        return f"section_{idx}_{safe_id}"

    def _compute_groups(
        self,
    ) -> List[Tuple[str, Optional[str], List[Tuple[int, str, str, Optional[str]]]]]:
        """Walk nav_items into groups. Each group: (group_id, label, items).

        - A labelled `add_divider(label)` opens a new group with id `navgroup_N`.
        - An empty `add_divider()` emits a separator-line sentinel
          (group_id=`__line__`, label=None, items=[]) — only meaningful inside
          the inner sidebar.
        - Items registered before the first labelled divider go into a
          synthetic group with id=`navgroup_root`, label=None.
        """
        groups: list = []
        current_label: Optional[str] = None
        current_items: list = []
        has_labelled = False

        def _flush() -> None:
            nonlocal current_label, current_items, has_labelled
            if current_items or has_labelled:
                if has_labelled:
                    gid = f"navgroup_{sum(1 for g in groups if g[0].startswith('navgroup_') and g[0] != 'navgroup_root')}"
                else:
                    gid = "navgroup_root"
                groups.append((gid, current_label, current_items))
            current_label = None
            current_items = []
            has_labelled = False

        for idx, (name, content, icon) in enumerate(self.nav_items):
            if content == "__DIVIDER__":
                label = name.replace("__DIVIDER__", "")
                if label:
                    _flush()
                    current_label = label
                    has_labelled = True
                else:
                    _flush()
                    groups.append(("__line__", None, []))
            else:
                current_items.append((idx, name, content, icon))
        _flush()
        return groups

    def _generate_outer_sidebar_html(self, groups) -> str:
        """Master pane — one entry per labelled group."""
        html = '<nav class="outer-nav">'
        for gid, label, _items in groups:
            if gid == "__line__" or label is None:
                continue
            html += (
                f'<a class="outer-nav-item" data-group-id="{gid}" '
                f'href="javascript:void(0);" onclick="setActiveGroup(\'{gid}\')">'
                f'<span class="outer-nav-label">{label}</span></a>'
            )
        html += '</nav>'
        return html

    def _generate_inner_sidebar_html(self, groups) -> str:
        """Detail pane — per-group nav-item containers, only one visible at a time."""
        html = '<nav class="inner-nav">'
        for gid, label, items in groups:
            if gid == "__line__":
                html += '<div class="nav-divider-line"></div>'
                continue
            # Un-grouped items (navgroup_root) render in their own container too;
            # they're tied to the synthetic root group_id and are visible when
            # that group is active. If only labelled groups exist, this branch
            # is never hit.
            container_class = "inner-nav-group"
            if label is None and gid == "navgroup_root":
                container_class += " navgroup-root"
            html += f'<div class="{container_class}" data-group-id="{gid}">'
            for idx, name, _content, icon in items:
                html += self._render_nav_item(idx, name, icon, group_id=gid)
            html += '</div>'
        html += '</nav>'
        return html

    def _render_nav_item(
        self,
        idx: int,
        name: str,
        icon: Optional[str],
        *,
        group_id: Optional[str] = None,
    ) -> str:
        section_id = self._generate_section_id(name, idx)
        icon_html = f'<span class="nav-icon">{icon}</span>' if icon else ''
        display_name = re.sub(r' - (?:Composite )?Vintage .+$', '', name)
        group_attr = f' data-group-id="{group_id}"' if group_id else ''
        return (
            f'<a href="#{section_id}" class="nav-item" '
            f'data-section-id="{section_id}"{group_attr}>'
            f'{icon_html}<span>{display_name}</span></a>'
        )

    def _generate_content_html(self, groups) -> str:
        """Sections grouped by their parent group_id; JS toggles visibility."""
        content_html = '<div class="content-wrapper">'
        for gid, _label, items in groups:
            if gid == "__line__":
                continue
            for idx, name, content, _icon in items:
                section_id = self._generate_section_id(name, idx)
                display_name = re.sub(r' - (?:Composite )?Vintage .+$', '', name)
                content_html += f'''
                <section id="{section_id}" class="content-section" data-group-id="{gid}">
                    <div class="section-header" onclick="this.parentElement.classList.toggle('collapsed')">
                        <span>{display_name}</span>
                        <span class="section-chevron">&#9662;</span>
                    </div>
                    <div class="section-content">{content}</div>
                </section>
                '''
        content_html += '</div>'
        return content_html

    def _generate_html(self) -> str:
        if not self.nav_items:
            raise ValueError("No content added. Use add_content() first.")

        c = self.colors
        groups = self._compute_groups()
        outer_sidebar_html = self._generate_outer_sidebar_html(groups)
        inner_sidebar_html = self._generate_inner_sidebar_html(groups)
        content_html = self._generate_content_html(groups)
        # First labelled group becomes the default-active page on load.
        first_active_group = next(
            (gid for gid, _l, _i in groups if gid.startswith("navgroup_")
             and gid != "navgroup_root"),
            "navgroup_root",
        )

        header_html = f'''
        <div class="header">
            <button class="menu-toggle" onclick="toggleSidebar()">&#9776;</button>
            <span class="header-title">{self.title}</span>
            <span class="header-timestamp">Updated: {datetime.now(ZoneInfo("America/Denver")).strftime("%a %b %d, %Y %H:%M MST")}</span>
        </div>
        ''' if self.show_header else ''

        html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{self.title}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@600;700&display=swap" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        html {{
            scroll-behavior: smooth;
        }}

        body {{
            font-family: 'IBM Plex Sans', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            font-size: 13px;
            background: {c['body_bg']};
            color: {c['body_text']};
            min-height: 100vh;
            background-attachment: fixed;
        }}

        body::before {{
            content: "";
            position: fixed;
            inset: 0;
            pointer-events: none;
            background: radial-gradient(900px circle at 85% 10%, rgba(76, 201, 240, 0.10), transparent 45%);
            z-index: -1;
        }}

        .header {{
            background: {c['header_bg']};
            color: {c['header_title_color']};
            padding: 8px 16px;
            display: flex;
            align-items: center;
            gap: 12px;
            font-weight: 700;
            font-size: 16px;
            border-bottom: 1px solid {c['header_border']};
            position: sticky;
            top: 0;
            z-index: 100;
            backdrop-filter: blur(8px);
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.25);
        }}

        .header-title {{
            font-family: 'Space Grotesk', 'IBM Plex Sans', sans-serif;
            letter-spacing: 0.2px;
        }}

        .menu-toggle {{
            background: none;
            border: none;
            color: {c['menu_toggle_color']};
            font-size: 18px;
            cursor: pointer;
            padding: 4px 8px;
        }}

        .menu-toggle:hover {{ background: {c['menu_toggle_hover_bg']}; border-radius: 3px; }}

        .header-timestamp {{
            font-size: 12px;
            color: {c['timestamp_text']};
            background: {c['timestamp_bg']};
            padding: 6px 12px;
            border-radius: 6px;
            font-weight: 500;
            margin-left: auto;
        }}

        .container {{
            display: flex;
            height: {'calc(100vh - 42px)' if self.show_header else '100vh'};
        }}

        .outer-sidebar {{
            width: 150px;
            min-width: 150px;
            background: {c['outer_sidebar_bg']};
            border-right: 1px solid {c['outer_sidebar_border']};
            overflow-y: auto;
            font-size: 12px;
            position: sticky;
            top: {'42px' if self.show_header else '0'};
            height: {'calc(100vh - 42px)' if self.show_header else '100vh'};
        }}

        .outer-nav {{ padding: 6px 0; }}

        .outer-nav-item {{
            display: block;
            padding: 12px 14px;
            cursor: pointer;
            color: {c['nav_color']};
            font-size: 12px;
            font-weight: 700;
            text-decoration: none;
            border-left: 3px solid transparent;
            transition: background 0.15s ease, color 0.15s ease;
            user-select: none;
            line-height: 1.3;
        }}

        .outer-nav-item:hover {{ background: {c['nav_hover_bg']}; }}

        .outer-nav-item.active {{
            background: {c['nav_active_bg']};
            color: {c['nav_active_color']};
            border-left-color: {c['nav_active_border']};
        }}

        .inner-sidebar {{
            width: {self.sidebar_width}px;
            min-width: {self.sidebar_width}px;
            background: {c['sidebar_bg']};
            border-right: 1px solid {c['sidebar_border']};
            overflow-y: auto;
            transition: width 0.2s, min-width 0.2s;
            font-size: 12px;
            position: sticky;
            top: {'42px' if self.show_header else '0'};
            height: {'calc(100vh - 42px)' if self.show_header else '100vh'};
        }}

        .inner-sidebar.collapsed {{
            width: 0;
            min-width: 0;
            overflow: hidden;
        }}

        .inner-nav {{ padding: 4px 0; }}

        /* Per-group nav lists — visible by default; JS hides non-active. */
        .inner-nav-group.group-hidden {{ display: none; }}

        .nav-item {{
            padding: 8px 12px;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 8px;
            color: {c['nav_color']};
            font-size: 13px;
            font-weight: 600;
            border-left: 3px solid transparent;
            text-decoration: none;
            transition: all 0.18s ease;
        }}

        .nav-item:hover {{ background: {c['nav_hover_bg']}; }}

        .nav-item.active {{
            background: {c['nav_active_bg']};
            color: {c['nav_active_color']};
            font-weight: 500;
            border-left: 3px solid {c['nav_active_border']};
        }}

        .nav-icon {{ font-size: 14px; }}

        .nav-divider-line {{
            height: 1px;
            background: {c['nav_divider_border']};
            margin: 8px 12px;
        }}

        .main-content {{
            flex: 1;
            overflow-y: auto;
            min-width: 0;
            scroll-behavior: smooth;
            background: {c['main_content_bg']};
        }}

        .content-wrapper {{
            padding: 8px 10px 20px 10px;
        }}

        .content-section {{
            border: 1px solid {c['section_border']};
            border-radius: 12px;
            margin: 8px 8px 16px 8px;
            background: rgba(11, 18, 32, 0.55);
            box-shadow: 0 10px 24px rgba(0, 0, 0, 0.22);
            overflow: hidden;
            opacity: 0;
            transform: translateY(8px);
            animation: sectionFadeIn 0.4s ease forwards;
        }}

        /* Sections are visible by default so plotly can size them correctly
           during inline-script render; JS hides non-active groups after load. */
        .content-section.group-hidden {{ display: none; }}

        .content-section:last-child {{
            margin-bottom: 8px;
        }}

        .content-section:nth-child(2) {{ animation-delay: 0.04s; }}
        .content-section:nth-child(3) {{ animation-delay: 0.08s; }}
        .content-section:nth-child(4) {{ animation-delay: 0.12s; }}
        .content-section:nth-child(5) {{ animation-delay: 0.16s; }}

        @keyframes sectionFadeIn {{
            from {{
                opacity: 0;
                transform: translateY(8px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}

        .section-header {{
            background: {c['section_header_bg']};
            color: {c['section_header_color']};
            padding: 10px 16px;
            font-weight: 700;
            font-size: 14px;
            font-family: 'Space Grotesk', 'IBM Plex Sans', sans-serif;
            letter-spacing: 0.2px;
            border-left: 3px solid {c['section_header_border']};
            border-bottom: 1px solid {c['section_border']};
            position: sticky;
            top: 0;
            z-index: 10;
            cursor: pointer;
            user-select: none;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }}

        .section-header:hover {{
            background: {c['section_header_bg']};
            filter: brightness(1.15);
        }}

        .section-chevron {{
            font-size: 12px;
            color: #6f8db1;
            transition: transform 0.2s ease;
            margin-left: 8px;
        }}

        .content-section.collapsed .section-content {{
            display: none;
        }}

        .content-section.collapsed .section-header {{
            border-bottom: none;
        }}

        .content-section.collapsed .section-chevron {{
            transform: rotate(-90deg);
        }}

        .section-content {{
            padding: 0 0 8px 0;
        }}

        .table-container {{
            border: 1px solid {c['table_container_border']};
            border-radius: 3px;
            margin: 8px;
        }}

        .search-input {{
            width: calc(100% - 16px);
            padding: 8px;
            margin: 8px;
            border: 1px solid {c['search_border']};
            border-radius: 3px;
            font-size: 12px;
            background: {c['search_bg']};
            color: {c['search_text']};
        }}

        .data-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 12px;
        }}

        .data-table th {{
            background: {c['table_th_bg']};
            color: {c['table_th_color']};
            padding: 8px;
            text-align: left;
            font-weight: 600;
            font-size: 11px;
            text-transform: uppercase;
            position: sticky;
            top: 0;
        }}

        .data-table td {{
            padding: 6px 8px;
            border-bottom: 1px solid {c['table_td_border']};
        }}

        .data-table tr.even {{ background: {c['table_even_bg']}; }}
        .data-table tr.odd {{ background: {c['table_odd_bg']}; }}
        .data-table tr:hover {{ background: {c['table_hover_bg']} !important; }}

        ::-webkit-scrollbar {{ width: 8px; height: 8px; }}
        ::-webkit-scrollbar-track {{ background: {c['scrollbar_track']}; }}
        ::-webkit-scrollbar-thumb {{ background: {c['scrollbar_thumb']}; border-radius: 4px; }}
        ::-webkit-scrollbar-thumb:hover {{ background: {c['scrollbar_thumb_hover']}; }}
    </style>
    <script src="https://cdn.plot.ly/plotly-3.4.0.min.js"></script>
</head>
<body>
    {header_html}

    <div class="container">
        <aside class="outer-sidebar" id="outerSidebar">
            {outer_sidebar_html}
        </aside>

        <aside class="inner-sidebar" id="innerSidebar">
            {inner_sidebar_html}
        </aside>

        <main class="main-content" id="mainContent">
            {content_html}
        </main>
    </div>

    <script>
        const DEFAULT_GROUP_ID = "{first_active_group}";

        function setActiveGroup(groupId) {{
            // Outer pane highlight
            document.querySelectorAll('.outer-nav-item').forEach(el => {{
                el.classList.toggle('active', el.dataset.groupId === groupId);
            }});
            // Inner sidebar — hide non-active group's nav-items
            document.querySelectorAll('.inner-nav-group').forEach(el => {{
                el.classList.toggle('group-hidden', el.dataset.groupId !== groupId);
            }});
            // Main content — hide non-active group's sections
            document.querySelectorAll('.content-section').forEach(el => {{
                el.classList.toggle('group-hidden', el.dataset.groupId !== groupId);
            }});
            // Reset main scroll and seed active nav-item to the first visible
            const main = document.getElementById('mainContent');
            if (main) main.scrollTop = 0;

            document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
            const firstNav = document.querySelector(
                '.inner-nav-group:not(.group-hidden) .nav-item'
            );
            if (firstNav) firstNav.classList.add('active');

            // Plotly charts that were rendered while their section was hidden
            // may have stale layout sizes — force a resize after they reappear.
            if (window.Plotly && Plotly.Plots && Plotly.Plots.resize) {{
                document.querySelectorAll(
                    '.content-section:not(.group-hidden) .js-plotly-plot'
                ).forEach(el => {{
                    try {{ Plotly.Plots.resize(el); }} catch (e) {{ /* noop */ }}
                }});
            }}
        }}

        document.addEventListener('DOMContentLoaded', function() {{
            // Activate the default group on load.
            setActiveGroup(DEFAULT_GROUP_ID);

            const mainContent = document.getElementById('mainContent');

            mainContent.addEventListener('scroll', function() {{
                const activeOuter = document.querySelector('.outer-nav-item.active');
                if (!activeOuter) return;
                const groupId = activeOuter.dataset.groupId;

                const sections = document.querySelectorAll(
                    '.content-section[data-group-id="' + groupId + '"]'
                );
                const navItems = document.querySelectorAll(
                    '.inner-nav-group:not(.group-hidden) .nav-item'
                );

                let current = '';
                const scrollPos = mainContent.scrollTop;
                sections.forEach(section => {{
                    const sectionTop = section.offsetTop - 100;
                    if (scrollPos >= sectionTop) {{
                        current = section.getAttribute('id');
                    }}
                }});

                navItems.forEach(item => {{
                    item.classList.toggle(
                        'active',
                        item.getAttribute('data-section-id') === current,
                    );
                }});
            }});

            // Inner nav-item clicks scroll smoothly to the section.
            document.querySelectorAll('.nav-item').forEach(item => {{
                item.addEventListener('click', function(e) {{
                    e.preventDefault();
                    const sectionId = this.getAttribute('data-section-id');
                    const section = document.getElementById(sectionId);
                    if (section) {{
                        section.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
                    }}
                }});
            }});
        }});

        function toggleSidebar() {{
            document.getElementById('innerSidebar').classList.toggle('collapsed');
        }}
    </script>
</body>
</html>
        """

        return html_template

    def build(self) -> str:
        return self._generate_html()

    def to_html(self, **kwargs) -> str:
        return self._generate_html()

    def save(self, filepath: str, **kwargs) -> str:
        html_content = self._generate_html()
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        return str(Path(filepath).absolute())
