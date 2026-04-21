from pathlib import Path

import numpy as np
import pandas as pd
from bokeh.io import curdoc
from bokeh.models import (
    ColorBar,
    ColumnDataSource,
    FactorRange,
    HoverTool,
    Legend,
    LegendItem,
    LinearColorMapper,
)
from bokeh.palettes import interp_palette

_BLUE_YELLOW = interp_palette(["#0d1b8e", "#0d6abf", "#00c8ff", "#ffff00"], 256)
from bokeh.plotting import figure
from bokeh.themes import Theme

from utils.bq_client import project_id, query

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


def chart_trends(
    year_min: int = 1993,
    year_max: int = 2023,
    show_trend: bool = True,
) -> tuple[figure, figure]:
    """Q1: how fast are SLA and SST rising globally? Returns (fig_sla, fig_sst)."""
    df = query(f"""
        SELECT year_month, avg_sla_m, avg_sst_celsius
        FROM `{project_id()}.mart.monthly_global_trends`
        ORDER BY year_month
    """)
    df["year_month"] = pd.to_datetime(df["year_month"])
    df = df[(df["year_month"].dt.year >= year_min) & (df["year_month"].dt.year <= year_max)]
    df["avg_sla_cm"] = df["avg_sla_m"] * 100

    x = df["year_month"]
    x_num = (x - x.min()).dt.days.values.astype(float)

    def _trend_fig(col, color, title, y_label):
        p = figure(
            x_axis_type="datetime",
            height=350,
            title=title,
            y_axis_label=y_label,
            toolbar_location="above",
            sizing_mode="stretch_width",
        )
        p.line(x, df[col], color=color, alpha=0.6)
        if show_trend and len(df) > 1:
            slope, intercept = np.polyfit(x_num, df[col].values, 1)
            p.line(x, slope * x_num + intercept, color=color, line_dash="dashed", line_width=2)
        p.add_tools(HoverTool(
            tooltips=[("date", "@x{%F}"), ("value", "@y{0.00}")],
            formatters={"@x": "datetime"},
        ))
        return p

    fig_sla = _trend_fig("avg_sla_cm",      _BLUE, "Sea Level Anomaly (SLA)",       "SLA (cm)")
    fig_sst = _trend_fig("avg_sst_celsius", _RED,  "Sea Surface Temperature (SST)", "SST (°C)")
    return fig_sla, fig_sst


def chart_decades() -> tuple[figure, figure]:
    """Q2: is the rise accelerating? Returns (fig_sla, fig_sst) bar charts by decade."""
    df = query(f"""
        SELECT
            CASE
                WHEN year BETWEEN 1993 AND 2002 THEN '1993-2002'
                WHEN year BETWEEN 2003 AND 2012 THEN '2003-2012'
                ELSE '2013-2023'
            END AS decade,
            AVG(avg_sla_m)       AS avg_sla_m,
            AVG(avg_sst_celsius) AS avg_sst_celsius
        FROM `{project_id()}.mart.monthly_global_trends`
        GROUP BY decade
        ORDER BY decade
    """)
    decades = df["decade"].tolist()

    def _bar(col, title, color, y_label):
        src = ColumnDataSource({"decade": decades, "value": df[col].tolist()})
        p = figure(
            x_range=FactorRange(*decades),
            height=320,
            title=title,
            y_axis_label=y_label,
            toolbar_location=None,
            sizing_mode="stretch_width",
        )
        p.vbar(x="decade", top="value", width=0.6, source=src, color=color, fill_alpha=0.8)
        p.add_tools(HoverTool(tooltips=[("decade", "@decade"), ("value", "@value{0.0000}")]))
        p.xgrid.grid_line_color = None
        return p

    df["avg_sla_cm"] = df["avg_sla_m"] * 100
    return (
        _bar("avg_sla_cm",      "Sea Level Anomaly (SLA) by decade",        _TEAL,  "avg SLA (cm)"),
        _bar("avg_sst_celsius", "Sea Surface Temperature (SST) by decade",  _PEACH, "avg SST (°C)"),
    )


def chart_zones() -> figure:
    """Q3: which latitude zones drive the warming signal?"""
    df = query(f"""
        SELECT year_month, latitude_zone, avg_sst_celsius
        FROM `{project_id()}.mart.latitude_zone_stats`
        ORDER BY year_month
    """)
    df["year_month"] = pd.to_datetime(df["year_month"])

    p = figure(
        x_axis_type="datetime",
        height=220,
        title="Sea Surface Temperature (SST) by latitude zone",
        y_axis_label="Sea surface temperature (°C)",
        toolbar_location="above",
        sizing_mode="stretch_width",
    )

    legend_items = []
    for zone, color in _ZONE_COLORS.items():
        sub = df[df["latitude_zone"] == zone].sort_values("year_month")
        src = ColumnDataSource({"x": sub["year_month"], "y": sub["avg_sst_celsius"], "zone": [zone] * len(sub)})
        line_r = p.line("x", "y", source=src, color=color, line_width=1.5)
        sq_r = p.square(
            [sub["year_month"].iloc[0]], [sub["avg_sst_celsius"].iloc[0]],
            fill_color=color, line_color=None, size=8, fill_alpha=0.9,
        )
        legend_items.append(LegendItem(label=zone, renderers=[sq_r, line_r]))

    legend = Legend(
        items=legend_items,
        location="top_left",
        click_policy="hide",
        background_fill_color="#1e2030",
        background_fill_alpha=0.92,
        border_line_color="#363a4f",
        label_text_color="#cad3f5",
    )
    p.add_layout(legend)
    p.add_tools(HoverTool(
        tooltips=[("zone", "@zone"), ("date", "@x{%F}"), ("SST", "@y{0.00} °C")],
        formatters={"@x": "datetime"},
    ))
    return p


def chart_sst_map(year: int, month: int) -> figure:
    """World map: SST heatmap on the 0.25° grid for a given year/month."""
    df = query(f"""
        SELECT latitude, longitude, sst_celsius
        FROM `{project_id()}.staging.sea_surface_temperature`
        WHERE year = {year} AND month = {month}
    """)

    # Build 2-D grid [lat × lon], NaN where no ocean data
    n_lat, n_lon = 721, 1441
    grid = np.full((n_lat, n_lon), np.nan)
    lat_idx = np.round((df["latitude"].values + 90.0) / 0.25).astype(int).clip(0, n_lat - 1)
    lon_idx = np.round((df["longitude"].values + 180.0) / 0.25).astype(int).clip(0, n_lon - 1)
    grid[lat_idx, lon_idx] = df["sst_celsius"].values

    mapper = LinearColorMapper(
        palette=_BLUE_YELLOW,
        low=-2.0,
        high=35.0,
        nan_color="#1e2030",
    )

    p = figure(
        title=f"Sea Surface Temperature — {year}-{month:02d}",
        x_axis_label="Longitude",
        y_axis_label="Latitude",
        x_range=(-180, 180),
        y_range=(-90, 90),
        height=260,
        toolbar_location="above",
        sizing_mode="stretch_width",
    )
    p.image(
        image=[grid],
        x=-180, y=-90,
        dw=360, dh=180,
        color_mapper=mapper,
    )
    p.add_layout(
        ColorBar(color_mapper=mapper, label_standoff=12, title="SST (°C)"),
        "right",
    )
    return p
