from pathlib import Path

import numpy as np
import pandas as pd
from bokeh.io import curdoc
from bokeh.layouts import gridplot
from bokeh.models import ColumnDataSource, FactorRange, HoverTool, LinearAxis, Range1d
from bokeh.plotting import figure
from bokeh.themes import Theme

from utils.bq_client import query

curdoc().theme = Theme(
    filename=Path(__file__).parents[1] / "themes" / "catppuccin_macchiato.yaml"
)

# Catppuccin Macchiato palette
_BLUE    = "#8aadf4"
_RED     = "#ed8796"
_TEAL    = "#8bd5ca"
_PEACH   = "#f5a97f"
_GREEN   = "#a6da95"
_MAUVE   = "#c6a0f6"
_SKY     = "#91d7e3"

_ZONES = ["Arctic", "N. Temperate", "Tropical", "S. Temperate", "Antarctic"]
_ZONE_COLORS = dict(zip(_ZONES, [_SKY, _BLUE, _PEACH, _GREEN, _MAUVE]))


def chart_trends() -> figure:
    """Q1: how fast are SLA and SST rising globally?"""
    df = query("""
        SELECT year_month, avg_sla_m, avg_sst_celsius
        FROM mart.monthly_global_trends
        ORDER BY year_month
    """)
    df["year_month"] = pd.to_datetime(df["year_month"])
    x = df["year_month"]
    x_num = (x - x.min()).dt.days.values.astype(float)

    p = figure(
        x_axis_type="datetime",
        height=380,
        title="Q1 — Global ocean trends (1993–2023)",
        y_axis_label="Sea level anomaly (m)",
        toolbar_location="above",
        sizing_mode="stretch_width",
    )
    p.extra_y_ranges = {"sst": Range1d(
        df["avg_sst_celsius"].min() - 0.5,
        df["avg_sst_celsius"].max() + 0.5,
    )}
    p.add_layout(LinearAxis(y_range_name="sst", axis_label="Sea surface temperature (°C)"), "right")

    p.line(x, df["avg_sla_m"],       color=_BLUE, alpha=0.4, legend_label="SLA (m)")
    p.line(x, df["avg_sst_celsius"], color=_RED,  alpha=0.4, legend_label="SST (°C)", y_range_name="sst")

    for col, color, yr_name in [
        ("avg_sla_m",       _BLUE, None),
        ("avg_sst_celsius", _RED,  "sst"),
    ]:
        slope, intercept = np.polyfit(x_num, df[col].values, 1)
        trend = slope * x_num + intercept
        kw = {"y_range_name": yr_name} if yr_name else {}
        p.line(x, trend, color=color, line_dash="dashed", line_width=2, **kw)

    p.legend.location = "top_left"
    p.add_tools(HoverTool(
        tooltips=[("date", "@x{%F}"), ("value", "@y{0.0000}")],
        formatters={"@x": "datetime"},
    ))
    return p


def chart_decades():
    """Q2: is the rise accelerating? Decadal averages for SLA and SST."""
    df = query("""
        SELECT
            CASE
                WHEN year BETWEEN 1993 AND 2002 THEN '1993-2002'
                WHEN year BETWEEN 2003 AND 2012 THEN '2003-2012'
                ELSE '2013-2023'
            END AS decade,
            AVG(avg_sla_m)       AS avg_sla_m,
            AVG(avg_sst_celsius) AS avg_sst_celsius
        FROM mart.monthly_global_trends
        GROUP BY decade
        ORDER BY decade
    """)
    decades = df["decade"].tolist()

    def _bar(col, title, color, y_label):
        src = ColumnDataSource({"decade": decades, "value": df[col].tolist()})
        p = figure(
            x_range=FactorRange(*decades),
            height=300,
            title=title,
            y_axis_label=y_label,
            toolbar_location=None,
            sizing_mode="stretch_width",
        )
        p.vbar(x="decade", top="value", width=0.6, source=src, color=color, fill_alpha=0.8)
        p.add_tools(HoverTool(tooltips=[("decade", "@decade"), ("value", "@value{0.0000}")]))
        p.xgrid.grid_line_color = None
        return p

    p_sla = _bar("avg_sla_m",       "Sea level anomaly by decade (m)",        _TEAL,  "avg SLA (m)")
    p_sst = _bar("avg_sst_celsius", "Sea surface temperature by decade (°C)", _PEACH, "avg SST (°C)")
    return gridplot([[p_sla, p_sst]], sizing_mode="stretch_width")


def chart_zones() -> figure:
    """Q3: which latitude zones drive the warming signal?"""
    df = query("""
        SELECT year_month, latitude_zone, avg_sst_celsius
        FROM mart.latitude_zone_stats
        ORDER BY year_month
    """)
    df["year_month"] = pd.to_datetime(df["year_month"])

    p = figure(
        x_axis_type="datetime",
        height=380,
        title="Q3 — SST by latitude zone (1993–2023)",
        y_axis_label="Sea surface temperature (°C)",
        toolbar_location="above",
        sizing_mode="stretch_width",
    )
    for zone, color in _ZONE_COLORS.items():
        sub = df[df["latitude_zone"] == zone].sort_values("year_month")
        p.line(sub["year_month"], sub["avg_sst_celsius"],
               color=color, line_width=1.5, legend_label=zone)

    p.legend.location = "top_left"
    p.legend.click_policy = "hide"
    p.add_tools(HoverTool(
        tooltips=[("date", "@x{%F}"), ("SST", "@y{0.00} °C")],
        formatters={"@x": "datetime"},
    ))
    return p
