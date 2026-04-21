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
  - key: CDS_API_KEY
  - key: gcs-data-lake
    inject_as: GCS_DATA_LAKE
@bruin"""

import json
import os
import tempfile
import warnings
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

DATASET = "satellite-sea-surface-temperature"
CACHE_DIR = Path(__file__).parents[2] / "data" / "cache" / "sst"


def _iter_months(start: date, end: date):
    current = date(start.year, start.month, 1)
    stop = date(end.year, end.month, 1)
    while current <= stop:
        yield current.year, f"{current.month:02d}"
        month = current.month % 12 + 1
        year = current.year + (current.month == 12)
        current = date(year, month, 1)


def _fetch_month_cds(year: int, month: str) -> pd.DataFrame:
    import cdsapi
    import xarray as xr

    cached_zip = CACHE_DIR / f"{year}_{month}.zip"

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        nc_dir = tmp / "nc"
        nc_dir.mkdir()

        if not cached_zip.exists():
            client = cdsapi.Client(
                quiet=True,
                key=os.environ["CDS_API_KEY"],
                url="https://cds.climate.copernicus.eu/api",
            )
            try:
                client.retrieve(
                    DATASET,
                    {
                        "variable": "all",
                        "version": "3_0",
                        "processinglevel": "level_4",
                        "sensor_on_satellite": "combined_product",
                        "temporal_resolution": "monthly",
                        "year": [str(year)],
                        "month": [month],
                    },
                    target=str(cached_zip),
                )
            except Exception as exc:
                raise RuntimeError(f"CDS download failed for SST {year}-{month}: {exc}") from exc

        with zipfile.ZipFile(cached_zip) as zf:
            zf.extractall(nc_dir)

        nc_files = sorted(nc_dir.glob("*.nc"))
        ds = xr.open_dataset(nc_files[0], engine="netcdf4")
        ds = ds[["analysed_sst", "sea_ice_fraction"]]
        ds = ds.rename({"lat": "latitude", "lon": "longitude"})
        ds["analysed_sst"] = ds["analysed_sst"] - 273.15
        df = ds.to_dataframe().reset_index()
        df = df.dropna(subset=["analysed_sst"])
        df = df.rename(columns={"analysed_sst": "sst_celsius"})
        df["year"] = pd.array([year] * len(df), dtype="int32")
        df["month"] = pd.array([int(month)] * len(df), dtype="int32")
        return df[["year", "month", "latitude", "longitude", "sst_celsius", "sea_ice_fraction"]]


def _bq_client(sa_info):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_info(sa_info)
    return bigquery.Client(credentials=credentials, project=sa_info["project_id"])


def materialize():
    start = date.fromisoformat(os.environ["BRUIN_START_DATE"])
    end = date.fromisoformat(os.environ["BRUIN_END_DATE"])

    if os.environ.get("GCS_DATA_LAKE"):
        # Prod: load parquet files from GCS directly into BigQuery via Load Job API
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

        # Return empty DataFrame — Load Job already handled ingestion.
        # Explicit dtypes prevent Bruin from recreating the table with STRING columns.
        return pd.DataFrame({
            "year":             pd.Series(dtype="int32"),
            "month":            pd.Series(dtype="int32"),
            "latitude":         pd.Series(dtype="float32"),
            "longitude":        pd.Series(dtype="float32"),
            "sst_celsius":      pd.Series(dtype="float32"),
            "sea_ice_fraction": pd.Series(dtype="float32"),
        })

    # Dev: download directly from CDS into DuckDB (raw 0.05° grid, no regridding)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    frames = [_fetch_month_cds(y, m) for y, m in _iter_months(start, end)]
    return pd.concat(frames, ignore_index=True)
