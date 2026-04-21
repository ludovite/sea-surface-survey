# Sea Surface Survey

A batch data engineering pipeline that ingests global ocean surface data from ESA satellite observations, transforms it through a multi-layer warehouse, and exposes climate trends on a dashboard.

## Problem Description

Can we measure the acceleration of climate change from 30 years of satellite data? This project answers three questions: (1) how fast are global ocean temperatures and sea levels rising? (2) is the rise accelerating? (3) which latitude zones drive the signal?

Sea level anomaly (SLA) and sea surface temperature (SST) are two of the most direct indicators of climate change. ESA satellites have measured both monthly since 1993 at global scale — but the raw NetCDF files from Copernicus are large, format-specific, and impractical to query directly.

This project builds a production-ready pipeline that:
- Downloads monthly satellite observations via the Copernicus CDS API
- Stores them as Parquet files in a GCS data lake
- Loads them into BigQuery and transforms raw grids into analytical tables aggregated by time and latitude zone
- Visualises global trends and zone-level breakdowns via a 2-tile dashboard

## Architecture

```
Copernicus CDS API
        │
        ▼
  Python ingest assets (Bruin)
  ├── raw.sea_level_anomaly        0.25° grid, monthly append
  └── raw.sea_surface_temperature  0.05° → regridded to 0.25°, monthly append
        │
        ▼
  SQL staging assets (Bruin)
  ├── staging.sea_level_anomaly
  └── staging.sea_surface_temperature   type-cast + year_month date key
        │
        ▼
  SQL mart assets (Bruin)
  ├── mart.monthly_global_trends    global avg SLA & SST per month
  └── mart.latitude_zone_stats      avg by latitude zone × month
        │
        ▼
  Dashboard (Bruin Cloud / bruin-viz)
```

Infrastructure is provisioned with **Terraform** (GCS bucket + BigQuery dataset on GCP).

## Tech Stack

| Layer | Dev | Prod |
|---|---|---|
| Orchestration | Bruin | Bruin |
| Data source | Copernicus CDS API | Copernicus CDS API |
| Warehouse | DuckDB (local file) | Google BigQuery |
| Data lake | — | Google Cloud Storage |
| Infrastructure | — | Terraform |
| Transformations | Bruin SQL | Bruin SQL |
| Dashboard | — | Streamlit + Bokeh (Hugging Face Spaces) |

**Python stack:** uv · xarray · netcdf4 · pandas · pyarrow

## Datasets

| Dataset | CDS identifier | Resolution | Variables |
|---|---|---|---|
| Sea Level Anomaly | `satellite-sea-level-global` | 0.25° | `sla` (m) |
| Sea Surface Temperature | `satellite-sea-surface-temperature` | 0.05° → 0.25° | `analysed_sst` (°C), `sea_ice_fraction` |

Coverage: global ocean, January 1993 – December 2023 (~360 months).  
Volume per month: ~570k grid points (SLA) · ~680k grid points (SST).

## Quickstart

### Prerequisites

- Python 3.11+, [uv](https://docs.astral.sh/uv/)
- [Bruin CLI](https://bruin-data.github.io/bruin/) (`brew install bruin-data/tap/bruin` or equivalent)
- [Copernicus CDS account](https://cds.climate.copernicus.eu/) + API key
- GCP account with a service account (prod only)

### 1. Clone and install

```bash
git clone https://github.com/<you>/sea-surface-survey
cd sea-surface-survey
uv sync
```

### 2. Configure environment

Create `.envrc` (gitignored) with the following variables:

```bash
# Copernicus CDS API key — format: <UID>:<TOKEN>
# Find your UID and token at https://cds.climate.copernicus.eu/profile
export CDS_API_KEY="<uid>:<token>"

# GCP (prod only)
export GOOGLE_CREDENTIALS="$HOME/.config/creds/<your-sa>.json"
export GOOGLE_APPLICATION_CREDENTIALS="$GOOGLE_CREDENTIALS"
export PROJECT_NAME="<gcp-project-id>"
export GCP_LOCATION="europe-west9"
export GCP_BUCKET="<gcs-bucket-name>"
export GCP_DATASET="<bq-dataset-name>"

# Terraform (prod only)
export TF_VAR_project="${PROJECT_NAME}"
export TF_VAR_region="${GCP_LOCATION}"
export TF_VAR_gcs_bucket_name="${GCP_BUCKET}"
export TF_VAR_bq_dataset_name="${GCP_DATASET}"
```

Then `direnv allow` (or `source .envrc`).

### 3. Provision infrastructure (prod only)

```bash
cd terraform
terraform init
terraform apply
```

### 4. Run the pipeline

**Dev — single month (DuckDB):**

```bash
mkdir -p data
bruin run . --start-date "1993-01-01" --end-date "1993-01-31"
```

**Prod — single month (BigQuery + GCS):**

```bash
bruin run . --environment prod --force --start-date "1993-01-01" --end-date "1993-01-31"
```

**Prod — backfill multiple months:**

```bash
bruin run . --environment prod --force --start-date "1993-01-01" --end-date "1993-06-30"
```

### 5. Monitor pipeline runs

`bruin` provides a local pipeline UI (asset graph, run history, row counts):

```bash
bruin-viz parse .   # generate the pipeline graph (run once, or after asset changes)
bruin-viz serve     # open http://localhost:8001
```

## Pipeline Details

### Materialization strategies

| Layer | Strategy | Notes |
|---|---|---|
| `raw.*` | `append` | One row-set per month, idempotent via CDS cache |
| `staging.*` | `delete+insert` on `year_month` | Replaces a month on re-run |
| `mart.*` | `delete+insert` on `year_month` | Replaces a month on re-run |

### BigQuery partitioning & clustering

| Table | Partition | Cluster | Rationale |
|---|---|---|---|
| `staging.sea_surface_temperature` | `year_month` | `year, month` | Partition aligns with `delete+insert` (one month updated per run); clustering speeds up the monthly GROUP BY aggregation |
| `mart.monthly_global_trends` | `year_month` | `year, month` | Dashboard time-range queries prune to relevant partitions; clustering eliminates full scans on year/month filters |
| `mart.latitude_zone_stats` | `year_month` | `latitude_zone` | Same partition benefit; clustering on zone matches the dashboard's group-by-zone queries |

### Local CDS cache

Downloaded zip files are cached under `data/cache/sla/` and `data/cache/sst/` (gitignored). Re-running a month that was already downloaded skips the API call entirely.

### SST regridding

The SST dataset is delivered at 0.05° resolution (~5 km). It is aggregated to the SLA 0.25° grid in the `staging.sea_surface_temperature` SQL asset using `ROUND(lat / 0.25) * 0.25` + `AVG GROUP BY`, so both datasets share the same spatial keys for joins.

## Data Dashboard

The dashboard is a **Streamlit + Bokeh** app hosted on [Hugging Face Spaces](https://huggingface.co/spaces/ludovite/sea-surface-survey). It reads directly from the BigQuery mart tables and answers the three project questions:

| Tile | Table | Description |
|---|---|---|
| Q1 — Global trends | `mart.monthly_global_trends` | Dual time series of SLA and SST (1993–2023) with linear trend lines |
| Q2 — Decadal acceleration | `mart.monthly_global_trends` | Average SLA and SST per decade (1993–2002 · 2003–2012 · 2013–2023) |
| Q3 — Latitude zone breakdown | `mart.latitude_zone_stats` | SST per zone over time (Arctic · N. Temperate · Tropical · S. Temperate · Antarctic) |

## Repository Structure

```
.
├── assets/
│   ├── raw/            Python ingest assets (CDS API → GCS → BigQuery)
│   ├── staging/        SQL cleaning and regridding assets
│   ├── mart/           SQL analytical assets
│   └── setup/          DDL assets (table creation with partitioning)
├── streamlit-app/      Dashboard (Streamlit + Bokeh, deployed on HF Spaces)
├── terraform/          GCS bucket + BigQuery datasets
├── pipeline.yml        Bruin pipeline definition
└── pyproject.toml      Python dependencies
```
