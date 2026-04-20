# Sea Surface Survey

A batch data engineering pipeline that ingests global ocean surface data from ESA satellite observations, transforms it through a multi-layer warehouse, and exposes climate trends on a dashboard.

## Problem Description

Sea level anomaly (SLA) and sea surface temperature (SST) are two of the most critical indicators of climate change. Both are measured monthly by ESA satellites since 1993 at global scale with sub-degree spatial resolution — but the raw NetCDF files from Copernicus are large, format-specific, and impractical to query directly.

This project builds a production-ready pipeline that:
- Downloads monthly satellite observations via the Copernicus CDS API
- Loads them into a columnar data warehouse (DuckDB for dev, BigQuery for prod)
- Transforms raw grids into analytical tables aggregated by time and latitude zone
- Visualises global trends via a 2-tile dashboard

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
| Data lake / staging | — | Google Cloud Storage |
| Infrastructure | — | Terraform |
| Transformations | Bruin SQL | Bruin SQL |
| Dashboard | bruin-viz | Bruin Cloud |

**Python stack:** uv · xarray · netcdf4 · scipy · pandas · pyarrow

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
# Copernicus CDS — path to your ~/.cdsapirc file
export CDS_API_KEY="$HOME/.cdsapirc"

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
bruin-viz run . --start-date "1993-01-01" --end-date "1993-01-31"
```

**Dev — backfill multiple months:**

```bash
python - << 'EOF'
import subprocess, calendar
from datetime import date

start, end = date(1993, 1, 1), date(1994, 12, 1)
current = start
while current <= end:
    last = date(current.year, current.month, calendar.monthrange(current.year, current.month)[1])
    subprocess.run(['bruin-viz', 'run', '.', '--start-date', str(current), '--end-date', str(last)])
    current = date(current.year + (current.month == 12), current.month % 12 + 1, 1)
EOF
```

**Prod (BigQuery + GCS):**

```bash
bruin run . --environment prod --start-date "1993-01-01" --end-date "1993-01-31"
```

### 5. Monitor pipeline runs

`bruin-viz` provides a pipeline dashboard (asset graph, run history, row counts):

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

### Local CDS cache

Downloaded zip files are cached under `data/cache/sla/` and `data/cache/sst/` (gitignored). Re-running a month that was already downloaded skips the API call entirely.

### SST regridding

The SST dataset is delivered at 0.05° resolution (~5 km). It is bilinearly interpolated onto the SLA 0.25° grid using `xarray.interp` (requires scipy) so both datasets share the same spatial keys for joins.

## Data Dashboard

The analytical dashboard (2 tiles) is served via **Bruin Cloud** and reads from the mart tables:

| Tile | Table | Description |
|---|---|---|
| Global monthly trends | `mart.monthly_global_trends` | Time series of global average SLA and SST |
| Latitude zone breakdown | `mart.latitude_zone_stats` | Averages by zone (Arctic, N. Temperate, Tropical, S. Temperate, Antarctic) |

> `bruin-viz serve` is a separate pipeline monitoring UI (asset graph, run history, row counts) — it is not the data dashboard.

## Repository Structure

```
.
├── assets/
│   ├── raw/            Python ingest assets (CDS API → warehouse)
│   ├── staging/        SQL cleaning assets
│   └── mart/           SQL analytical assets
├── terraform/          GCS bucket + BigQuery dataset
├── pipeline.yml        Bruin pipeline definition
└── pyproject.toml      Python dependencies
```
