"""Build the Western Hub Renewables Ramp Study PDF report.

Run from project root:  python modelling/backtests/_build_west_hub_pdf.py
"""
import io
import os
from pathlib import Path
from datetime import date as Date

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak,
    Table, TableStyle,
)

ROOT = Path(__file__).resolve().parent.parent.parent
RESULTS_CSV = ROOT / "modelling/backtests/output/west_hub_renewables_backtest_2026-04-27.csv"
DAILY_PARQUET = ROOT / "modelling/backtests/output/west_hub_renewables_daily_2026-04-27.parquet"
SOLAR_PARQUET = ROOT / "modelling/data/cache/pjm_solar_generation_by_area_hourly.parquet"
WIND_PARQUET = ROOT / "modelling/data/cache/pjm_wind_generation_by_area_hourly.parquet"
TMP_CHARTS = ROOT / "modelling/backtests/output/_pdf_charts"
TMP_CHARTS.mkdir(parents=True, exist_ok=True)
OUT_PDF = ROOT / "modelling/backtests/output/west_hub_renewables_report.pdf"

AREAS = ["WEST", "SOUTH", "MIDATL"]
AREA_COLORS = {"WEST": "#e65100", "SOUTH": "#388e3c", "MIDATL": "#7b1fa2"}

# ----- Load data -----
print("Loading data...")
results = pd.read_csv(RESULTS_CSV)
solar = pd.read_parquet(SOLAR_PARQUET)
solar["datetime_beginning_ept"] = pd.to_datetime(solar["datetime_beginning_ept"])
solar["date"] = solar["datetime_beginning_ept"].dt.normalize()
solar["hour_ending"] = solar["datetime_beginning_ept"].dt.hour + 1
wind = pd.read_parquet(WIND_PARQUET)
wind["datetime_beginning_ept"] = pd.to_datetime(wind["datetime_beginning_ept"])
wind["date"] = wind["datetime_beginning_ept"].dt.normalize()
wind["hour_ending"] = wind["datetime_beginning_ept"].dt.hour + 1


# ----- CHART 1 -----
def chart_capacity_growth():
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.4))
    for ax, df, val, title in [
        (axes[0], solar, "solar_generation_mw", "Solar avg MW by year (PJM zonal areas)"),
        (axes[1], wind, "wind_generation_mw", "Wind avg MW by year (PJM zonal areas)"),
    ]:
        d = df[df["area"].isin(AREAS)].copy()
        d["year"] = d["date"].dt.year
        agg = d.groupby(["year", "area"])[val].mean().unstack()
        for area in AREAS:
            if area in agg.columns:
                ax.plot(agg.index, agg[area], marker="o", color=AREA_COLORS[area],
                        label=area, linewidth=2)
        ax.set_title(title, fontsize=10, weight="bold")
        ax.set_xlabel("Year"); ax.set_ylabel("Avg MW")
        ax.legend(loc="upper left", fontsize=8)
        ax.grid(alpha=0.3)
    plt.tight_layout()
    p = TMP_CHARTS / "capacity_growth.png"
    plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close()
    return p


# ----- CHART 2 -----
def chart_diurnal():
    cutoff = solar["date"].max() - pd.Timedelta(days=365)
    fig, axes = plt.subplots(2, 2, figsize=(9, 5.6))
    for row, (df, val, source) in enumerate([
        (solar, "solar_generation_mw", "Solar"),
        (wind, "wind_generation_mw", "Wind"),
    ]):
        d = df[(df["date"] >= cutoff) & (df["area"].isin(AREAS))].copy()
        d["season"] = np.where(d["date"].dt.month.isin([5,6,7,8,9]), "summer", "winter")
        for col, season in enumerate(["summer", "winter"]):
            ax = axes[row, col]
            ds = d[d["season"] == season]
            agg = ds.groupby(["area", "hour_ending"])[val].mean().unstack(level=0)
            for area in AREAS:
                if area in agg.columns:
                    ax.plot(agg.index, agg[area], color=AREA_COLORS[area],
                            label=area, linewidth=2, marker="o", markersize=3)
            ax.set_title(f"{source} - {season} avg profile (last 12mo)",
                         fontsize=10, weight="bold")
            ax.set_xlabel("HE"); ax.set_ylabel("MW")
            ax.set_xticks([1,4,8,12,16,20,24])
            ax.legend(loc="upper left", fontsize=8); ax.grid(alpha=0.3)
            ax.axvspan(17, 20, alpha=0.08, color="red")
    plt.tight_layout()
    p = TMP_CHARTS / "diurnal.png"
    plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close()
    return p


# ----- CHART 3 -----
def chart_top_western():
    renew_cats = [c for c in results["category"].unique()
                  if any(s in c for s in ["Solar Ramp","Wind Ramp","Renewables","Cross-Region","Penetration","Coincidence"])]
    sub = results[(results["target"]=="western") & (results["category"].isin(renew_cats))].copy()
    sub = sub.dropna(subset=["corr_level"])
    top = sub.reindex(sub["corr_level"].abs().sort_values(ascending=False).index).head(20)
    fig, ax = plt.subplots(figsize=(9, 6))
    bar_colors = ["#2e7d32" if v > 0 else "#c62828" for v in top["corr_level"]]
    y = np.arange(len(top))
    ax.barh(y, top["corr_level"], color=bar_colors)
    ax.set_yticks(y); ax.set_yticklabels(top["indicator"], fontsize=8)
    ax.invert_yaxis()
    ax.axvline(0, color="black", linewidth=0.5)
    ax.set_xlabel("Pearson correlation with next-day Western Hub on-peak DA LMP")
    ax.set_title("Top 20 renewable indicators (Western Hub target)", fontsize=11, weight="bold")
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    p = TMP_CHARTS / "top_western.png"
    plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close()
    return p


# ----- CHART 4 -----
def chart_coincidence():
    daily = pd.read_parquet(DAILY_PARQUET)
    if "da_western_avg_onpeak" not in daily.columns:
        return None
    rows = []
    for area in AREAS:
        for src in ["solar", "wind"]:
            col = f"{src}_{area.lower()}_avg_super_peak"
            if col in daily.columns:
                joined = daily[[col, "da_western_avg_onpeak"]].dropna()
                if len(joined) > 50:
                    corr = joined[col].corr(joined["da_western_avg_onpeak"])
                    rows.append({"area": area, "source": src, "corr": corr})
    df = pd.DataFrame(rows)
    fig, ax = plt.subplots(figsize=(8, 3.5))
    pivot = df.pivot(index="area", columns="source", values="corr").reindex(AREAS)
    x = np.arange(len(pivot))
    w = 0.35
    ax.bar(x - w/2, pivot["solar"], w, color="#f57c00", label="Solar (super-peak avg)")
    ax.bar(x + w/2, pivot["wind"], w, color="#0288d1", label="Wind (super-peak avg)")
    ax.axhline(0, color="black", linewidth=0.5)
    ax.set_xticks(x); ax.set_xticklabels(pivot.index)
    ax.set_ylabel("Corr w/ same-day West Hub on-peak DA")
    ax.set_title("Renewable super-peak (HE 17-20) coincidence with Western Hub price",
                 fontsize=11, weight="bold")
    ax.legend(); ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    p = TMP_CHARTS / "coincidence.png"
    plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close()
    return p


print("Generating charts...")
ch_capacity = chart_capacity_growth()
ch_diurnal = chart_diurnal()
ch_top = chart_top_western()
ch_coinc = chart_coincidence()
print("  charts OK")

# =============================================================================
# PDF BUILD
# =============================================================================

styles = getSampleStyleSheet()

H1 = ParagraphStyle("H1", parent=styles["Heading1"], fontSize=18, leading=22,
                    textColor=colors.HexColor("#0d3b66"), spaceAfter=10, spaceBefore=14)
H2 = ParagraphStyle("H2", parent=styles["Heading2"], fontSize=13, leading=16,
                    textColor=colors.HexColor("#1565c0"), spaceAfter=6, spaceBefore=10)
H3 = ParagraphStyle("H3", parent=styles["Heading3"], fontSize=11, leading=14,
                    textColor=colors.HexColor("#37474f"), spaceAfter=4, spaceBefore=6,
                    fontName="Helvetica-Bold")
BODY = ParagraphStyle("BODY", parent=styles["BodyText"], fontSize=10, leading=14,
                      alignment=TA_JUSTIFY, spaceAfter=6)
SMALL = ParagraphStyle("SMALL", parent=styles["BodyText"], fontSize=9, leading=12)
CODE = ParagraphStyle("CODE", parent=styles["BodyText"], fontSize=9, leading=12,
                      fontName="Courier", textColor=colors.HexColor("#222"),
                      backColor=colors.HexColor("#f4f4f4"), borderPadding=4,
                      leftIndent=8, rightIndent=8)
TITLE = ParagraphStyle("TITLE", parent=styles["Title"], fontSize=24, leading=28,
                       textColor=colors.HexColor("#0d3b66"), spaceAfter=12, alignment=TA_CENTER)
SUBTITLE = ParagraphStyle("SUBTITLE", parent=styles["Heading2"], fontSize=14,
                          leading=18, textColor=colors.HexColor("#555"),
                          alignment=TA_CENTER, spaceAfter=8)
CAPTION = ParagraphStyle("CAPTION", parent=styles["BodyText"], fontSize=8,
                         leading=10, textColor=colors.HexColor("#666"),
                         alignment=TA_CENTER, spaceAfter=12)

doc = SimpleDocTemplate(str(OUT_PDF), pagesize=letter,
                        leftMargin=0.7*inch, rightMargin=0.7*inch,
                        topMargin=0.7*inch, bottomMargin=0.7*inch)
story = []

def add_table(data, col_widths=None, font_size=8.5, zebra=True):
    t = Table(data, colWidths=col_widths, hAlign="LEFT", repeatRows=1)
    style = [
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#0d3b66")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.white),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,-1), font_size),
        ("ALIGN", (0,0), (-1,0), "CENTER"),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ("TOPPADDING", (0,0), (-1,-1), 4),
        ("LINEBELOW", (0,0), (-1,0), 0.5, colors.black),
        ("GRID", (0,0), (-1,-1), 0.25, colors.HexColor("#cccccc")),
    ]
    if zebra:
        for i in range(1, len(data)):
            if i % 2 == 0:
                style.append(("BACKGROUND", (0,i), (-1,i), colors.HexColor("#f5f7fa")))
    t.setStyle(TableStyle(style))
    return t


# COVER
story.append(Spacer(1, 1.2*inch))
story.append(Paragraph("PJM Western Hub", TITLE))
story.append(Paragraph("Regional Solar &amp; Wind Ramp Study", SUBTITLE))
story.append(Spacer(1, 0.4*inch))
story.append(Paragraph(
    "A backtest of regional renewable ramp indicators against next-day "
    "Western Hub on-peak day-ahead LMP, with full indicator catalog and findings.",
    ParagraphStyle("cov", parent=BODY, alignment=TA_CENTER, fontSize=11,
                   textColor=colors.HexColor("#555"))))
story.append(Spacer(1, 0.6*inch))

cover_facts = [
    ["Data window", "2022-01-01 to 2026-04-27 (1,578 days)"],
    ["Indicators tested", "796"],
    ["Hub targets", "5 (Western primary; Eastern, AEP-Dayton, N.Illinois, Dominion secondary)"],
    ["Target definition", "Next-day on-peak DA LMP (HE 8-23 weekday non-NERC-holiday avg)"],
    ["Regional renewable source", "pjm.solar_generation_by_area, pjm.wind_generation_by_area"],
    ["PJM zonal regions used", "WEST, SOUTH, MIDATL"],
    ["Report generated", Date.today().isoformat()],
]
story.append(add_table(cover_facts, col_widths=[2.0*inch, 4.5*inch], font_size=10))
story.append(PageBreak())

# EXECUTIVE SUMMARY
story.append(Paragraph("Executive Summary", H1))
story.append(Paragraph(
    "This study tests whether daily-aggregated metrics built from realized hourly "
    "solar and wind generation, broken down by PJM zonal region (WEST, SOUTH, MIDATL), "
    "predict next-day Western Hub on-peak day-ahead LMPs. We construct ~800 indicators "
    "covering magnitude, ramp rate, time-of-day windows, sunrise/sunset shape, "
    "cross-region differentials, penetration shares, and renewable-load coincidence. "
    "Each indicator is tested against five hub targets (Western Hub primary) using "
    "Pearson correlation, R<super>2</super>, direction accuracy, Spearman rank IC, and a "
    "top-quintile minus bottom-quintile next-day price spread.", BODY))

story.append(Paragraph("Headline finding", H3))
story.append(Paragraph(
    "Renewable ramp magnitudes <i>as standalone features</i> add little predictive power "
    "to a Western Hub day-ahead model. The strongest renewable indicator caps at "
    "|corr| = 0.121 with R<super>2</super> = 0.015. Western Hub day-ahead prices are "
    "dominated by load, gas, and lagged LMP self-correlations - renewable ramp signals "
    "appear to be substantially absorbed into the DA forward curve before clearing.", BODY))

story.append(Paragraph("What does carry signal", H3))
top_findings = [
    ["Indicator", "Corr", "QSpread", "What it captures"],
    ["solar_west_minus_south_dropoff_16_19", "+0.121", "+$14", "Cross-region asymmetry: WEST solar drops harder than SOUTH"],
    ["solar_midatl_min", "+0.110", "+$25", "Clear-day proxy (highest QSpread)"],
    ["solar_midatl_max_hourly_ramp_up", "-0.111", "-$12", "Faster MIDATL morning ramp = price suppression"],
    ["solar_west_coincidence_west_load", "-0.092", "-$3", "WEST solar landing during WEST peak load"],
    ["wind_west_coincidence_west_load", "-0.086", "-$13", "WEST wind during peak (largest wind QSpread)"],
    ["solar_midatl_share_of_pjm", "-0.075", "-$12", "MIDATL solar concentration suppresses Western prices"],
]
story.append(add_table(top_findings,
    col_widths=[2.6*inch, 0.6*inch, 0.65*inch, 3.0*inch], font_size=8))

story.append(Spacer(1, 8))
story.append(Paragraph("Practical takeaway", H3))
story.append(Paragraph(
    "Drop ramp <i>magnitude</i> features from a DA model. Keep three things instead: "
    "(1) <b>renewable-load coincidence ratios</b> (the only reliable real-time renewable signal), "
    "(2) <b>cross-region differentials</b> (the only signal where regional asymmetry surfaces "
    "above noise), and (3) <b>penetration shares</b> (capturing geographic concentration of "
    "supply). To find genuine alpha in renewable signals, repeat this study using "
    "<i>forecast errors</i> (actual minus DA-cutoff forecast) rather than realized levels.", BODY))
story.append(PageBreak())

# METHODOLOGY
story.append(Paragraph("Methodology", H1))

story.append(Paragraph("Target", H2))
story.append(Paragraph(
    "For each calendar date T, the target is the average DA LMP across hour-ending "
    "8 through 23 on date T+1, computed from <code>pjm_lmps_hourly</code> filtered to "
    "<code>market = 'da'</code>. The target is masked when T+1 is a weekend or NERC "
    "holiday, leaving roughly 1,100 valid weekday-non-holiday targets across the 1,578-day window. "
    "Each indicator is tested against this target across five hub slugs - western, eastern, "
    "aep_dayton, n_illinois, dominion - with Western Hub as the primary focus.", BODY))

story.append(Paragraph("Data sources", H2))
data_table = [
    ["Source", "Granularity", "Purpose", "Coverage"],
    ["pjm_lmps_hourly", "hour x hub x market", "Targets and LMP profile features", "2014-2026"],
    ["pjm.solar_generation_by_area", "hour x area", "Regional solar (key)", "2019-04-2026"],
    ["pjm.wind_generation_by_area", "hour x area", "Regional wind (key)", "2011-01-2026"],
    ["pjm_load_rt_hourly", "hour x region", "Load magnitude / ramps", "2014-2026"],
    ["pjm_fuel_mix_hourly", "hour (RTO)", "RTO renewables, net-load baseline", "2020-2026"],
    ["pjm_outages_actual_daily", "day x region", "Capacity tightness", "2020-2026"],
    ["ice_python_next_day_gas_hourly", "hour", "Gas marginal cost", "2020-2026"],
    ["wsi_pjm_hourly_observed_temp", "hour x station", "Weather (HDD, CDD, peak temp)", "2014-2026"],
    ["pjm_dates_daily", "day", "Calendar (weekend, holiday, season)", "2010-2026"],
]
story.append(add_table(data_table,
    col_widths=[2.1*inch, 1.4*inch, 2.2*inch, 1.0*inch], font_size=8.5))
story.append(Spacer(1, 6))
story.append(Paragraph(
    "<b>Why 2022-01-01 onward?</b> Solar in PJM West grew from 14 MW average in 2019 "
    "to 1,325 MW average in 2026. Earlier years would dilute regional signals because "
    "the underlying capacity didn't yet exist. The 2022 cutoff balances solar relevance "
    "with sample size.", BODY))

story.append(Paragraph("Statistical tests per indicator", H2))
story.append(Paragraph(
    "Each indicator is evaluated against each target with five complementary metrics:", BODY))
metric_table = [
    ["Metric", "Definition", "What it tells us"],
    ["Pearson corr (level)", "corr(x_t, target_t+1)", "Linear association with next-day price level"],
    ["Pearson corr (change)", "corr(x_t, target_t+1 - target_t)", "Whether the indicator predicts price changes"],
    ["R-squared", "OLS R<super>2</super> from x_t alone -> target_t+1", "Variance explained by single feature"],
    ["Direction accuracy", "P(sign(x - median) == sign(target - median))", "Above/below median agreement"],
    ["Spearman IC", "rank corr(x_t, target_t+1)", "Robust to outliers and nonlinearity"],
    ["QSpread", "mean(target | x in top 20%) - mean(target | x in bottom 20%)", "Trading-style quintile spread, $/MWh"],
]
story.append(add_table(metric_table,
    col_widths=[1.4*inch, 2.5*inch, 2.6*inch], font_size=8.5))
story.append(Spacer(1, 6))
story.append(Paragraph(
    "Indicators with fewer than 60 valid (non-null) days are dropped. Indicators with "
    "fewer than 100 non-null observations across the entire frame are not tested.", SMALL))
story.append(PageBreak())


# PART 1
story.append(Paragraph("Part 1 - PJM Renewable Dynamics", H1))

story.append(Paragraph("Capacity by zonal region (recent 90-day average)", H2))
cap_table = [
    ["Area", "Solar avg MW (90d)", "Solar peak MW (90d)", "Wind avg MW (90d)", "Wind peak MW (90d)"],
    ["WEST", "1,490", "7,164", "4,180", "8,435"],
    ["SOUTH", "1,150", "5,117", "170", "471"],
    ["MIDATL", "350", "1,879", "410", "1,022"],
]
story.append(add_table(cap_table,
    col_widths=[1.0*inch, 1.4*inch, 1.4*inch, 1.4*inch, 1.4*inch], font_size=9))
story.append(Spacer(1, 6))
story.append(Paragraph(
    "<b>Two zonal asymmetries shape Western Hub renewable exposure:</b><br/>"
    "1. <b>Wind is concentrated in WEST.</b> ~85% of PJM zonal wind capacity sits in the "
    "Western footprint (AEP, ATSI, ComEd, DEOK). When wind production swings, it swings "
    "in WEST.<br/>"
    "2. <b>Solar is split between WEST and SOUTH.</b> WEST solar grew rapidly post-2023 "
    "(from ~14 MW to ~1.3 GW avg). SOUTH (Dominion) has been larger longer. MIDATL holds "
    "comparatively little despite being load-heavy.", BODY))

story.append(Spacer(1, 6))
story.append(Image(str(ch_capacity), width=6.7*inch, height=2.5*inch))
story.append(Paragraph("Figure 1 - Annual average MW by area, solar (left) and wind (right)", CAPTION))

story.append(PageBreak())
story.append(Paragraph("Diurnal profiles (last 12 months)", H2))
story.append(Paragraph(
    "The shape of solar and wind by hour-of-day differs sharply between regions and "
    "seasons. Red shading marks the super-peak window (HE 17-20) where Western Hub "
    "on-peak prices are most often set.", BODY))
story.append(Image(str(ch_diurnal), width=6.7*inch, height=4.2*inch))
story.append(Paragraph(
    "Figure 2 - Average hourly profile by area (top: solar; bottom: wind) for "
    "summer (May-Sep) and winter (Oct-Apr). Red band = super-peak window HE 17-20.",
    CAPTION))
story.append(Paragraph(
    "<b>What the profiles reveal:</b><br/>"
    "- Solar peaks at HE 13-14 then collapses through the super-peak window. By HE 20 "
    "it is effectively zero in all regions, regardless of season.<br/>"
    "- Wind in WEST is the only meaningful renewable contributor at HE 17-20. SOUTH "
    "and MIDATL wind are too small to materially affect the super-peak.<br/>"
    "- Summer wind in WEST shows weak diurnal seasonality (slight afternoon dip), "
    "winter wind is more variable but higher in absolute terms.<br/>"
    "- The dropoff <i>asymmetry</i> between regions is what matters: WEST solar drops "
    "from ~5 GW to 0 over 4 hours; MIDATL drops from ~1.5 GW to 0 over the same window. "
    "These ramp rates differ by ~3x in absolute MW, creating regional flow imbalances.",
    BODY))
story.append(PageBreak())

story.append(Paragraph("Coincidence with Western Hub same-day price", H2))
story.append(Paragraph(
    "If renewables suppress Western Hub LMPs, we expect a negative correlation between "
    "the average MW each region produces during HE 17-20 and the same-day Western Hub "
    "on-peak DA price. This chart tests that for solar and wind separately:", BODY))
story.append(Image(str(ch_coinc), width=6.7*inch, height=3.0*inch))
story.append(Paragraph(
    "Figure 3 - Same-day correlation between regional super-peak (HE 17-20) "
    "renewable output and Western Hub on-peak DA LMP.", CAPTION))
story.append(Paragraph(
    "<b>Reading the chart:</b> All bars are negative - both fuels suppress same-day "
    "Western Hub price. WEST is consistently the strongest region for both, reflecting "
    "where the capacity sits. <b>Solar shows stronger same-day coincidence than wind</b> "
    "in this metric (WEST solar -0.079 vs WEST wind -0.058) because solar has more "
    "day-to-day variability than wind in absolute MW terms. Note that WEST solar at HE 17-18 "
    "is genuinely a multi-GW resource on summer days (mean ~1,000 MW, P90 over 3,000 MW), "
    "only collapsing fully by HE 20. SOUTH and MIDATL effects are weak, consistent with "
    "their smaller footprints. <b>Caveat:</b> this is the simple super-peak average MW "
    "correlation. The coincidence <i>ratio</i> form (gen / load) flips the ranking - WEST "
    "wind narrowly wins there (corr -0.127 vs -0.120 for solar same-day), and the "
    "QSpread for the wind ratio is much larger (-$13/MWh vs -$3 for solar), because "
    "wind/load varies more usefully day-to-day than the small solar/load ratio.", BODY))
story.append(PageBreak())

# PART 2
story.append(Paragraph("Part 2 - Indicator Catalog", H1))
story.append(Paragraph(
    "Each indicator is constructed from realized hourly data, aggregated to one value "
    "per day. The 12 categories below cover all 796 indicators tested. For each "
    "category we give the construction recipe, the physical meaning, and the strongest "
    "Western Hub result observed.", BODY))

story.append(Paragraph("Time windows used throughout", H3))
windows_table = [
    ["Window", "Hour-Ending range", "Use case"],
    ["On-peak", "HE 8-23 (16 hours)", "Standard PJM on-peak window, weekday non-NERC-holiday"],
    ["Off-peak", "HE 1-7, 24", "Overnight + early morning"],
    ["Morning ramp", "HE 5-9", "Load ramps from overnight low into morning peak"],
    ["Evening ramp", "HE 15-21", "Load ramps from midday into evening peak"],
    ["Super-peak", "HE 17-20", "Tightest hours; sets most on-peak price action"],
    ["Midday", "HE 11-14", "Solar peak, load valley"],
    ["Overnight", "HE 1-5", "Wind dominates, load floor"],
]
story.append(add_table(windows_table,
    col_widths=[1.2*inch, 1.7*inch, 3.6*inch], font_size=9))
story.append(Spacer(1, 12))


def cat_block(title, definition_html, formula_html, captures_html, results_rows):
    story.append(Paragraph(title, H3))
    story.append(Paragraph(f"<b>Construction.</b> {definition_html}", BODY))
    if formula_html:
        story.append(Paragraph(formula_html, CODE))
    story.append(Paragraph(f"<b>Captures.</b> {captures_html}", BODY))
    if results_rows:
        header = ["Indicator", "Corr", "R<super>2</super>", "QSpread $", "Read"]
        rows = [header] + results_rows
        story.append(add_table(rows,
            col_widths=[2.5*inch, 0.55*inch, 0.5*inch, 0.7*inch, 2.55*inch], font_size=8))
    story.append(Spacer(1, 8))


cat_block(
    "1. Magnitude features (max, min, avg, std, range)",
    "Per-day summary statistics on the 24 hourly values for each (source, region) pair. "
    "<code>x_max</code>, <code>x_min</code>, <code>x_avg</code>, <code>x_std</code>, "
    "<code>x_range = x_max - x_min</code>.",
    "Example: solar_west_max = max(solar_WEST_HE1, ..., solar_WEST_HE24) for that day",
    "Whether a region had a 'big' renewable day in absolute terms. Range and std capture "
    "intraday volatility (how peaky the production profile was).",
    [
        ["solar_midatl_min", "+0.110", "0.012", "+24.9",
         "Higher daily minimum = clearer day = hot weather (load proxy)"],
        ["solar_midatl_max", "-0.087", "0.008", "-6.9", "More MIDATL solar capacity = some price suppression"],
        ["solar_midatl_range", "-0.087", "0.008", "-7.1", "Same as above; range tracks max"],
        ["wind_west_std", "-0.079", "0.006", "-13.1", "Higher daily wind volatility associated with lower DA"],
    ],
)

cat_block(
    "2. Window averages (on-peak / super-peak / midday / overnight / etc.)",
    "Average MW within a specific hour band. We compute on-peak (HE 8-23), super-peak "
    "(HE 17-20), midday (HE 11-14), morning (HE 5-9), evening (HE 15-21), overnight "
    "(HE 1-5), and offpeak (everything not on-peak).",
    "Example: wind_west_avg_super_peak = mean(wind_WEST at HE 17, 18, 19, 20)",
    "Whether renewables showed up <i>when it counted</i>. Super-peak average is the most "
    "directly relevant window for on-peak DA pricing.",
    [
        ["solar_midatl_avg_midday", "-0.084", "0.007", "-7.4", "MIDATL solar at noon: small suppression"],
        ["wind_west_avg_super_peak", "-0.017", "0.000", "-8.1", "Wind during HE 17-20: weak corr but $8 quintile spread"],
        ["renewables_west_avg_super_peak", "-0.038", "0.001", "-10.9", "Combined solar+wind at peak: $11 quintile spread"],
    ],
)

cat_block(
    "3. Hourly delta features (max ramp up, max ramp down, hourly std)",
    "Take the 24-hour series, compute <code>diff</code> between consecutive hours. "
    "Take the max (steepest hourly climb), min (steepest hourly drop), and std (overall "
    "hourly variability).",
    "diff_h = MW_h - MW_{h-1}; max_hourly_ramp_up = max(diff_h); max_hourly_ramp_down = min(diff_h)",
    "How fast the resource changed in any single hour. Most directly relevant for solar "
    "evening dropoff and wind sudden-calm events.",
    [
        ["solar_midatl_max_hourly_ramp_up", "-0.111", "0.012", "-12.3",
         "Steeper MIDATL morning ramp = more solar = lower price"],
        ["wind_south_max_hourly_ramp_up", "+0.096", "0.009", "+6.8", "Curious positive; likely SOUTH wind correlates with stress"],
        ["solar_midatl_hourly_ramp_std", "-0.093", "0.009", "-7.5", "More variable solar = generally bigger production"],
    ],
)

cat_block(
    "4. Multi-hour delta features (3-hour and 6-hour ramps)",
    "Same as hourly diffs but with 3-hour and 6-hour lookback. Captures sustained ramps "
    "that single-hour diffs would miss. Both up and down extremes are recorded.",
    "diff3_h = MW_h - MW_{h-3}; max_3hr_ramp_up = max(diff3); max_3hr_ramp_down = min(diff3)",
    "Sustained ramps over the operationally relevant 3-6 hour windows. Wind ramp-down "
    "events (sudden calms) typically take 3-6 hours to fully materialize.",
    [
        ["solar_midatl_max_3hr_ramp_up", "-0.097", "0.009", "-10.0", "MIDATL solar 3hr morning ramp"],
        ["wind_west_max_6hr_ramp_up", "-0.069", "0.005", "-11.0", "Big WEST wind ramp-ups associated with lower prices"],
        ["wind_west_max_3hr_ramp_down", "-0.018", "0.000", "-0.7", "Wind drop events do not lift Western Hub price (already priced)"],
    ],
)

cat_block(
    "5. Named ramps (specific hour-pair windows)",
    "Signed deltas between explicitly-named hour pairs that correspond to known operational "
    "events: morning load ramp, evening load ramp, super-peak window, valley-to-peak, "
    "midday-to-peak, post-peak, overnight.",
    "morning_5_9 = MW_HE9 - MW_HE5;  evening_16_20 = MW_HE20 - MW_HE16;  super_peak_17_20 = MW_HE20 - MW_HE17",
    "Direct measurement of how much a resource changed across an operationally-defined "
    "window. For solar, evening_16_20 is negative (solar drops). For load, the same "
    "window is positive (load rises into peak).",
    [
        ["solar_midatl_ramp_midday_to_peak_11_19", "+0.101", "0.010", "+11.8",
         "MIDATL solar at HE 19 minus HE 11 (always negative)"],
        ["solar_midatl_ramp_valley_to_peak_13_19", "+0.097", "0.009", "+9.9", "Same logic"],
        ["wind_south_ramp_post_peak_19_23", "+0.090", "0.008", "+8.2", "SOUTH wind after peak"],
    ],
)

cat_block(
    "6. Specific hour values (HE 8, 14, 17, 18, 19, 20)",
    "The raw MW value at specific hour endings, kept as features in their own right. "
    "Particularly useful for HE 17-20 which set on-peak pricing.",
    "Example: solar_west_he19 = solar_WEST at HE 19 on day T",
    "Avoids any aggregation: 'how much wind/solar was running at the exact hour the "
    "price gets set?' This is the most physically literal feature type.",
    [
        ["solar_midatl_he20", "-0.069", "0.005", "-5.4", "Solar at HE 20 (near zero in absolute terms)"],
        ["wind_west_he19", "-0.038", "0.001", "-7.1", "Wind at HE 19 - direct super-peak measurement"],
    ],
)

cat_block(
    "7. Solar-specific shape features (sunrise, sunset, dropoffs, asymmetry)",
    "Features that exploit solar's predictable daily shape: sunrise/sunset hour-endings "
    "(first/last HE with > 1 MW), daylight duration, specific dropoff windows, and the "
    "asymmetry between morning ramp-up and evening ramp-down.",
    ("sunrise_he = first HE where MW &gt; 1<br/>"
     "sunset_he = last HE where MW &gt; 1<br/>"
     "dropoff_17_20 = MW_HE17 - MW_HE20 (always positive for solar)<br/>"
     "ramp_asymmetry = (MW_HE12 - MW_HE20) / (MW_HE12 - MW_HE6)"),
    "Proxies for season (sunrise/sunset shift), cloud cover (lower max + slower ramps), "
    "and the duck-curve asymmetry that drives net-load steepening into evening peak.",
    [
        ["solar_west_minus_south_dropoff_16_19", "+0.121", "0.015", "+14.0",
         "<b>Top renewable indicator.</b> WEST evening dropoff harder than SOUTH = price stress"],
        ["solar_midatl_dropoff_14_20", "-0.083", "0.007", "-6.2", "Larger afternoon-to-evening MIDATL drop = bigger solar day"],
        ["solar_west_steepness", "+0.083", "0.007", "+1.0", "Range divided by hours from valley to peak"],
    ],
)

cat_block(
    "8. Cross-region differentials (WEST minus SOUTH, WEST minus MIDATL)",
    "Subtract one region's named ramp/dropoff from another's. Tests whether asymmetric "
    "regional behavior - the same fuel ramping at different rates in different zones - "
    "predicts Western Hub stress.",
    ("solar_west_minus_south_dropoff_16_19 = solar_west_dropoff_16_19 - solar_south_dropoff_16_19<br/>"
     "wind_west_minus_midatl_ramp_super_peak_17_20 = wind_west_ramp_super_peak_17_20 - wind_midatl_ramp_super_peak_17_20"),
    "Regional weather divergence (cloudy in WEST but sunny in SOUTH) creates asymmetric "
    "supply across PJM. Western Hub sits at the receiving end of the AEP/ATSI/ComEd "
    "footprint - if WEST loses generation while other zones don't, the resulting flow "
    "imbalance shows up as Western-specific congestion.",
    [
        ["solar_west_minus_south_dropoff_16_19", "+0.121", "0.015", "+14.0",
         "<b>Best cross-region signal.</b> WEST drops harder than SOUTH"],
        ["solar_west_minus_midatl_dropoff_16_19", "+0.091", "0.008", "-2.6",
         "WEST drops harder than MIDATL"],
        ["solar_west_minus_south_ramp_evening_16_20", "-0.054", "0.003", "-3.2",
         "Direction-flipped from dropoff (signs convention)"],
    ],
)

cat_block(
    "9. Penetration shares (zone fraction of PJM zonal total)",
    "Each zone's daily-average MW divided by the daily-average sum across the three "
    "zonal areas (WEST + SOUTH + MIDATL). Indicates where supply is geographically "
    "concentrated on a given day.",
    "solar_&lt;zone&gt;_share_of_pjm = solar_&lt;zone&gt;_avg / (solar_west_avg + solar_south_avg + solar_midatl_avg)",
    "Geographic supply concentration. When MIDATL holds a bigger share of solar (relative "
    "to its physical capacity), it's likely cloudy in WEST and clear in MIDATL. The "
    "implied flow geography is: load-heavy MIDATL self-supplies, less East-to-West "
    "import is needed, and Western Hub clears at relative discount.",
    [
        ["solar_midatl_share_of_pjm", "-0.075", "0.006", "-11.5",
         "Higher MIDATL solar share = lower Western Hub price"],
        ["wind_west_share_of_pjm", "-0.026", "0.001", "+0.0",
         "Wind share is too stable to vary much (WEST always dominates)"],
        ["solar_west_share_of_pjm", "+0.025", "0.001", "-5.8",
         "Higher WEST solar share = mild upward pressure (export congestion)"],
    ],
)

cat_block(
    "10. Coincidence ratios (renewable supply / load during super-peak)",
    "WEST renewable super-peak avg divided by WEST load super-peak avg. Captures whether "
    "renewable production aligned with the moments load was highest. A value of 1.0 "
    "would mean renewables met all super-peak load (impossible in practice).",
    ("solar_west_coincidence_west_load = solar_west_avg_super_peak / load_west_avg_super_peak<br/>"
     "wind_west_coincidence_west_load = wind_west_avg_super_peak / load_west_avg_super_peak"),
    "Production timing relative to load. Solar that arrives at midday but is gone by HE 19 "
    "doesn't help peak prices. Wind that blows during HE 17-20 directly suppresses them. "
    "Among all the renewable indicators tested, this is the most physically defensible signal.",
    [
        ["solar_west_coincidence_west_load", "-0.092", "0.009", "-2.7",
         "WEST solar timing match (small QSpread because solar is near-zero at peak)"],
        ["wind_west_coincidence_west_load", "-0.086", "0.007", "-12.7",
         "<b>Largest QSpread among wind indicators.</b> Wind during peak suppresses Western Hub"],
    ],
)

cat_block(
    "11. Combined renewables (solar + wind aggregates by region)",
    "Direct sum of solar and wind ramp/avg features per region. Captures total renewable "
    "behavior in a zone without weighting between fuels.",
    "renewables_west_avg_super_peak = solar_west_avg_super_peak + wind_west_avg_super_peak",
    "Total renewable contribution by region, recognizing that for Western Hub the marginal "
    "MW from solar or wind is fungible at the busbar.",
    [
        ["renewables_midatl_ramp_valley_to_peak_13_19", "+0.084", "0.007", "+13.5",
         "MIDATL renewable ramp from valley to evening peak"],
        ["renewables_west_avg_super_peak", "-0.038", "0.001", "-10.9",
         "Combined WEST renewables at peak: $11 QSpread"],
    ],
)

cat_block(
    "12. Day-over-day ramp changes (*_dod_chg)",
    "First differences of selected ramp features across days. Tests whether 'today's "
    "ramp is bigger than yesterday's' adds information beyond the absolute level.",
    "x_dod_chg = x_today - x_yesterday",
    "Whether the renewable trajectory is accelerating or decelerating relative to the "
    "previous day. In practice this washed out: the level matters more than the change.",
    [
        ["solar_west_ramp_evening_16_20_dod_chg", "-0.069", "0.005", "-",
         "Bigger evening ramp than yesterday: very weak signal"],
        ["solar_west_dropoff_17_20_dod_chg", "+0.045", "0.002", "-",
         "DoD change in evening dropoff"],
    ],
)
story.append(PageBreak())


# Top 20 chart
story.append(Paragraph("Visual: top 20 renewable indicators (Western Hub)", H2))
story.append(Image(str(ch_top), width=6.7*inch, height=4.5*inch))
story.append(Paragraph(
    "Figure 4 - Pearson correlation with next-day Western Hub on-peak DA LMP. "
    "Green = positive (indicator up implies price up), red = negative (indicator up "
    "implies price down). MIDATL solar features dominate the top by quantity but the "
    "<i>cross-region differential</i> at the very top is the only structurally clean signal.",
    CAPTION))
story.append(PageBreak())


# PART 3: HYPOTHESES
story.append(Paragraph("Part 3 - Hypotheses", H1))
story.append(Paragraph(
    "Seven hypotheses about how solar and wind drive Western Hub day-ahead prices, "
    "ranked by evidence strength. Each is tied to specific indicator results.", BODY))

hyps = [
    ("H1 - Western Hub renewable sensitivity runs through wind, not solar - but only with timing match.",
     "WEST holds ~85% of PJM zonal wind capacity but only ~50% of solar. The strongest wind "
     "signal is the coincidence ratio, not absolute MW (corr -0.086, QSpread -$13 - the largest "
     "wind quintile spread in the study). Solar in WEST cannot help on-peak prices because "
     "solar is functionally zero by HE 19-20 regardless of how much was produced midday."),
    ("H2 - Coincidence dominates magnitude across both fuels.",
     "For both WEST solar and WEST wind, coincidence ratios (gen / load during super-peak) "
     "outperform absolute MW levels. New renewable capacity does not reduce Western Hub on-peak "
     "prices unless it produces during HE 17-20."),
    ("H3 - Cross-region asymmetry is the cleanest 'real' renewable signal Western Hub gives off.",
     "<code>solar_west_minus_south_dropoff_16_19</code> is the top renewable indicator overall "
     "(corr +0.121). Interpretation: when WEST solar drops harder than SOUTH solar - i.e., "
     "regional weather divergence - Western Hub stresses more than the rest of PJM, consistent "
     "with import congestion from Dominion when WEST self-supply weakens asymmetrically."),
    ("H4 - MIDATL solar features are weather proxies, not renewable signals.",
     "<code>solar_midatl_min</code> has the largest QSpread (+$25/MWh) of any renewable "
     "indicator. But MIDATL holds only ~350 MW of solar capacity - far too little to "
     "physically move PJM-wide prices. The signal is real, but it's tagging clear/hot summer "
     "days where load is the actual driver."),
    ("H5 - Penetration concentration drives interregional flow patterns.",
     "<code>solar_midatl_share_of_pjm</code> is negative (-0.075). When solar is concentrated "
     "in MIDATL relative to its capacity, MIDATL self-supplies, eastward import demand drops, "
     "the AEP/Western corridor relaxes, and Western Hub clears cheaper. The reverse "
     "(WEST-heavy share with MIDATL dim) implies WEST exports and corridor congestion."),
    ("H6 - Solar/wind ramps as levels carry no alpha - they're already priced into the DA curve.",
     "Every ramp magnitude indicator (morning ramp, evening dropoff, max 3hr ramp down) plateaus "
     "at corr 0.05-0.10 with R<super>2</super> &lt; 0.015. PJM and Meteologica forecast these "
     "ramps reasonably well; the DA auction prices the expected ramp before clearing. The "
     "remaining variance lives in <i>forecast errors</i>, not realized ramp levels."),
    ("H7 - Wind ramp-down events are invisible at the daily-average level.",
     "<code>wind_west_max_3hr_ramp_down</code> has corr ~ 0. Two possibilities: (a) tail events "
     "are too rare to move daily averages; (b) the market handles them efficiently. Most likely "
     "(a) - wind ramp events on the order of 5-10 days per year, not enough to register in a "
     "1,578-day correlation. Conditional analysis (only days with extreme ramps) would be needed."),
]
for title, text in hyps:
    story.append(Paragraph(title, H3))
    story.append(Paragraph(text, BODY))
    story.append(Spacer(1, 4))
story.append(PageBreak())


# PART 4
story.append(Paragraph("Part 4 - Practical Recommendations", H1))

story.append(Paragraph("For a Western Hub DA model: what to keep, what to drop", H2))
recs_table = [
    ["Keep", "Drop"],
    ["Renewable-load coincidence ratios (solar_west_coincidence_west_load, wind_west_coincidence_west_load)",
     "Solar/wind ramp magnitudes as levels (morning ramp, evening dropoff, max 3hr ramp - all R^2 < 0.02)"],
    ["Cross-region differentials (solar_west_minus_south_dropoff_16_19 etc.)",
     "Day-over-day ramp changes (*_dod_chg - washed out by noise)"],
    ["Penetration shares (zone share of PJM zonal total)",
     "MIDATL solar magnitudes as 'renewable' signals (they are weather proxies; use weather features directly)"],
    ["RTO super-peak renewables averages (combined solar+wind, HE 17-20)",
     "Single-hour values without a coincidence interpretation"],
]
story.append(add_table(recs_table, col_widths=[3.4*inch, 3.4*inch], font_size=9))

story.append(Spacer(1, 12))
story.append(Paragraph("Where the real alpha probably sits", H2))
story.append(Paragraph(
    "Across 796 indicators tested, no single renewable feature explains more than ~1.5% "
    "of next-day on-peak DA variance. This is a strong indication that the DA forward "
    "curve absorbs most renewable ramp expectations before clearing. The natural next "
    "step is to recompute every indicator on <i>forecast errors</i> rather than realized "
    "values.", BODY))
story.append(Paragraph(
    "Concretely: pull the DA-cutoff snapshots of regional load, solar, and wind "
    "forecasts (PJM and/or Meteologica). For each day T+1, compute "
    "<code>(actual_ramp_T+1) - (DA_cutoff_forecast_ramp_T+1_issued_T)</code>. The residual "
    "is the genuine surprise - the part the market did not price. That's where renewable "
    "alpha for a DA model would live.", BODY))

story.append(Paragraph("Limitations", H2))
limitations = [
    "<b>Capacity drift.</b> WEST solar grew 100x from 2019 to 2026. A correlation computed "
    "across 2022-2026 averages over a non-stationary capacity base. The most recent year is "
    "more representative of current dynamics than the earliest year.",
    "<b>Realized vs forecast.</b> All renewable indicators here use realized hourly generation. "
    "A real DA model would use forecasts available before market close. The realized version is "
    "an upper bound on what could be predicted from contemporaneous renewable shape.",
    "<b>Tail events.</b> Wind ramp-down events and extreme weather days are rare enough that "
    "averaging across all 1,578 days masks any conditional alpha that exists in stress periods.",
    "<b>Same-day causality.</b> Some signals (e.g., MIDATL solar min) likely operate through "
    "third-variable confounds (weather -> load -> price) rather than directly. The study does not "
    "control for these.",
    "<b>Targets are weekday-only.</b> Weekend on-peak dynamics are excluded by the next-day "
    "filter. Weekends behave differently (lower load, different gas-fired marginal unit) and "
    "would need separate analysis.",
]
for s in limitations:
    story.append(Paragraph(f"- {s}", BODY))

story.append(Spacer(1, 14))
story.append(Paragraph("Files produced by this study", H2))
files_table = [
    ["File", "Contents"],
    ["west_hub_renewables_backtest_2026-04-27.html", "Interactive report with charts and filterable tables"],
    ["west_hub_renewables_backtest_2026-04-27.csv", "Long-form indicator x target results"],
    ["west_hub_renewables_daily_2026-04-27.parquet", "Daily feature frame (1,578 days x 798 columns)"],
    ["pjm_solar_generation_by_area_hourly.parquet", "Cached realized regional solar (2020-2026)"],
    ["pjm_wind_generation_by_area_hourly.parquet", "Cached realized regional wind (2020-2026)"],
    ["west_hub_renewables_report.pdf", "This document"],
]
story.append(add_table(files_table, col_widths=[3.0*inch, 3.8*inch], font_size=8.5))


print(f"Building PDF -> {OUT_PDF}")
doc.build(story)
print(f"  size: {OUT_PDF.stat().st_size / 1024:.0f} KB")
