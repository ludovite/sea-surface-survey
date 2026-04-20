import streamlit as st

from utils.charts import chart_decades, chart_trends, chart_zones

st.set_page_config(
    page_title="Sea Surface Survey",
    page_icon="🌊",
    layout="wide",
)

st.title("🌊 Sea Surface Survey")
st.caption(
    "30 years of ESA satellite observations (1993–2023) · "
    "Sea level anomaly & sea surface temperature · Global ocean"
)

st.divider()

st.subheader("Q1 — How fast are global ocean temperatures and sea levels rising?")
st.bokeh_chart(chart_trends(), use_container_width=True)

st.divider()

st.subheader("Q2 — Is the rise accelerating?")
st.caption("Decadal averages — each bar represents the mean over a 10-year period.")
st.bokeh_chart(chart_decades(), use_container_width=True)

st.divider()

st.subheader("Q3 — Which latitude zones drive the signal?")
st.caption("Click a zone in the legend to show/hide it.")
st.bokeh_chart(chart_zones(), use_container_width=True)
