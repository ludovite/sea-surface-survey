import os

import streamlit as st
from streamlit_bokeh import streamlit_bokeh

from utils.charts import chart_decades, chart_trends, chart_zones

st.set_page_config(
    page_title="Sea Surface Survey",
    page_icon="🌊",
    layout="wide",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:opsz,wght@14..32,300..700&display=swap');
html, body, [class*="css"], .stMarkdown, .stCaption { font-family: 'Inter', sans-serif; font-size: 17px; }
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

st.subheader("Q1 — How fast are global ocean temperatures and sea levels rising?")
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

st.subheader("Q2 — Is the rise accelerating?")
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

st.subheader("Q3 — Which latitude zones drive the signal?")
st.caption("Click a zone in the legend to show/hide it.")
try:
    streamlit_bokeh(chart_zones())
except Exception as e:
    st.warning(f"Chart unavailable: {e}")
