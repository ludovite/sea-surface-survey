"""@bruin
name: raw.sea_surface_temperature
connection: warehouse
tags:
  - ingestion

materialization:
  type: table
  strategy: append

columns:
  - name: year
    type: INTEGER
    description: Year of the observation
    primary_key: true
  - name: month
    type: INTEGER
    description: Month of the observation (1–12)
    primary_key: true
  - name: latitude
    type: FLOAT
    description: Latitude in degrees (-90 to 90), native 0.05° grid
  - name: longitude
    type: FLOAT
    description: Longitude in degrees (-180 to 180), native 0.05° grid
  - name: sst_celsius
    type: FLOAT
    description: Sea surface temperature in °C (converted from Kelvin, ESA SST CCI L4)
  - name: sea_ice_fraction
    type: FLOAT
    description: Sea ice fraction (0–1)

depends:
  - raw.sea_surface_temperature_to_gcs

secrets:
  - key: gcs-data-lake
    inject_as: GCS_DATA_LAKE
@bruin"""

import json
import os
from datetime import date

import pandas as pd


def _iter_months(start: date, end: date):
    current = date(start.year, start.month, 1)
    stop = date(end.year, end.month, 1)
    while current <= stop:
        yield current.year, f"{current.month:02d}"
        month = current.month % 12 + 1
        year = current.year + (current.month == 12)
        current = date(year, month, 1)


def _bq_client(sa_info):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_info(sa_info)
    return bigquery.Client(credentials=credentials, project=sa_info["project_id"])


def materialize():
    start = date.fromisoformat(os.environ["BRUIN_START_DATE"])
    end = date.fromisoformat(os.environ["BRUIN_END_DATE"])

    creds_data = json.loads(os.environ["GCS_DATA_LAKE"])
    if "service_account_json" in creds_data:
        sa_info = json.loads(creds_data["service_account_json"])
    else:
        with open(creds_data["service_account_file"]) as f:
            sa_info = json.load(f)

    from google.cloud import bigquery
    client = _bq_client(sa_info)
    bucket_name = creds_data["bucket_name"]
    table_id = f"{sa_info['project_id']}.raw.sea_surface_temperature"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    for y, m in _iter_months(start, end):
        uri = f"gs://{bucket_name}/raw/sea_surface_temperature/{y}/{m}.parquet"
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        print(f"Loaded {uri} into {table_id}")

    # Return empty DataFrame — BigQuery Load Job already handled the ingestion
    return pd.DataFrame(columns=["year", "month", "latitude", "longitude", "sst_celsius", "sea_ice_fraction"])
