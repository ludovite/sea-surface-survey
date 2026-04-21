---
title: Sea Surface Survey
emoji: 🌊
colorFrom: blue
colorTo: indigo
sdk: streamlit
sdk_version: 1.45.0
app_file: app.py
pinned: false
short_description: sea level anomaly & sea surface temperature trends
license: mit
---

# Sea Surface Survey Dashboard

30 years of ESA satellite observations (1993–2023) visualised in three charts:

1. **Global trends** — SLA and SST time series with linear trend lines
2. **Decadal acceleration** — average SLA and SST per decade (1993–2002 · 2003–2012 · 2013–2023)
3. **Latitude zone breakdown** — SST per zone (Arctic · N. Temperate · Tropical · S. Temperate · Antarctic)

## Data sources

- [ESA Sea Level CCI](https://cds.climate.copernicus.eu/datasets/satellite-sea-level-global) — `satellite-sea-level-global`
- [ESA SST CCI](https://cds.climate.copernicus.eu/datasets/satellite-sea-surface-temperature) — `satellite-sea-surface-temperature`

Pipeline: [sea-surface-survey](https://github.com/ludovite/sea-surface-survey)

## Secret required

Set `GCP_SA_JSON` in the Space secrets with the content of a GCP service account JSON
that has `BigQuery Data Viewer` and `BigQuery Job User` roles on the `global-sea-survey` project.
