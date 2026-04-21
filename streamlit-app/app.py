import os
import time

import streamlit as st
from streamlit_bokeh import streamlit_bokeh

from utils.charts import chart_decades, chart_sst_map, chart_trends, chart_zones

st.set_page_config(
    page_title="Sea Surface Survey",
    page_icon="🌊",
    layout="wide",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:opsz,wght@14..32,300..700&display=swap');
html, body, [class*="css"], .stMarkdown, .stCaption { font-family: 'Inter', sans-serif; font-size: 18px; }
</style>
""", unsafe_allow_html=True)

st.title("🌊 Sea Surface Survey")
st.caption(
    "30 years of ESA satellite observations (1993–2023) · "
    "Sea level anomaly & sea surface temperature · Global ocean"
)

if not os.environ.get("GCP_SA_JSON"):
    st.info(
        "**No BigQuery connection configured.** "
        "Set the `GCP_SA_JSON` environment variable (service account JSON) "
        "to connect to the production database. "
        "See the README for setup instructions.",
        icon="ℹ️",
    )
    charts = [f"./img/chart{i}.png" for i in range(1, 4)]
    available = [p for p in charts if os.path.exists(p)]
    if available:
        for path in available:
            st.image(path)
    st.stop()

st.divider()

# --- World map ---

_ALL_MONTHS = [f"{y}-{m:02d}" for y in range(1993, 2024) for m in range(1, 13)]

if "map_idx" not in st.session_state:
    st.session_state.map_idx = 0
if "map_playing" not in st.session_state:
    st.session_state.map_playing = False

st.subheader("🌍 Sea Surface Temperature — World Map")

slider_col, btn_col = st.columns([6, 1])
with slider_col:
    selected = st.select_slider(
        "Month",
        options=_ALL_MONTHS,
        value=_ALL_MONTHS[st.session_state.map_idx],
        label_visibility="collapsed",
    )
    st.session_state.map_idx = _ALL_MONTHS.index(selected)
with btn_col:
    label = "⏸ Pause" if st.session_state.map_playing else "▶ Play"
    if st.button(label, use_container_width=True):
        st.session_state.map_playing = not st.session_state.map_playing
        st.rerun()

year, month = int(selected[:4]), int(selected[5:])
try:
    streamlit_bokeh(chart_sst_map(year, month))
except Exception as e:
    st.warning(f"Map unavailable: {e}")

if st.session_state.map_playing:
    time.sleep(1)
    st.session_state.map_idx = (st.session_state.map_idx + 1) % len(_ALL_MONTHS)
    st.rerun()

st.divider()

# --- Q1 ---

st.subheader("How fast are global ocean temperatures and sea levels rising?")
try:
    ctrl1, ctrl2 = st.columns([4, 1])
    with ctrl1:
        year_range = st.slider("Year range", 1993, 2023, (1993, 2023))
    with ctrl2:
        show_trend = st.toggle("Trend line", value=True)

    fig_sla, fig_sst = chart_trends(*year_range, show_trend)
    col1, col2 = st.columns(2)
    with col1:
        streamlit_bokeh(fig_sla)
    with col2:
        streamlit_bokeh(fig_sst)
except Exception as e:
    st.warning(f"Chart unavailable: {e}")

st.divider()

# --- Q2 ---

st.subheader("Is the rise accelerating?")
st.caption("Decadal averages — each bar represents the mean over a 10-year period.")
try:
    fig_sla, fig_sst = chart_decades()
    col1, col2 = st.columns(2)
    with col1:
        streamlit_bokeh(fig_sla)
    with col2:
        streamlit_bokeh(fig_sst)
except Exception as e:
    st.warning(f"Chart unavailable: {e}")

st.divider()

# --- Q3 ---

st.subheader("Which latitude zones drive the signal?")
st.caption("Click a zone in the legend to show/hide it.")
try:
    streamlit_bokeh(chart_zones())
except Exception as e:
    st.warning(f"Chart unavailable: {e}")
