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
    description: Latitude in degrees (-90 to 90), 0.25° grid (regridded from 0.05°)
  - name: longitude
    type: FLOAT
    description: Longitude in degrees (-180 to 180), 0.25° grid (regridded from 0.05°)
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
from io import BytesIO
from pathlib import Path

import cdsapi
import numpy as np
import pandas as pd
import xarray as xr

warnings.filterwarnings("ignore")

DATASET = "satellite-sea-surface-temperature"
CACHE_DIR = Path(__file__).parents[2] / "data" / "cache" / "sst"
GCS_PREFIX = "raw/sea_surface_temperature"

TARGET_LAT = np.arange(-89.875, 90.0, 0.25)
TARGET_LON = np.arange(-179.875, 180.0, 0.25)


def _iter_months(start: date, end: date):
    current = date(start.year, start.month, 1)
    stop = date(end.year, end.month, 1)
    while current <= stop:
        yield current.year, f"{current.month:02d}"
        month = current.month % 12 + 1
        year = current.year + (current.month == 12)
        current = date(year, month, 1)


def _fetch_month_cds(year: int, month: str) -> pd.DataFrame:
    cached_zip = CACHE_DIR / f"{year}_{month}.zip"

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        nc_dir = tmp / "nc"
        nc_dir.mkdir()

        if not cached_zip.exists():
            client = cdsapi.Client(quiet=True)
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

        with zipfile.ZipFile(cached_zip) as zf:
            zf.extractall(nc_dir)

        nc_files = sorted(nc_dir.glob("*.nc"))
        ds = xr.open_dataset(nc_files[0], engine="netcdf4")
        ds = ds[["analysed_sst", "sea_ice_fraction"]]
        ds = ds.rename({"lat": "latitude", "lon": "longitude"})
        ds = ds.interp(latitude=TARGET_LAT, longitude=TARGET_LON, method="linear")
        ds["analysed_sst"] = ds["analysed_sst"] - 273.15
        df = ds.to_dataframe().reset_index()
        df = df.dropna(subset=["analysed_sst"])
        df = df.rename(columns={"analysed_sst": "sst_celsius"})
        df["year"] = year
        df["month"] = int(month)
        return df[["year", "month", "latitude", "longitude", "sst_celsius", "sea_ice_fraction"]]


def _gcs_client():
    creds_data = json.loads(os.environ["GCS_DATA_LAKE"])
    from google.cloud import storage
    from google.oauth2 import service_account

    if "service_account_json" in creds_data:
        sa_info = json.loads(creds_data["service_account_json"])
    else:
        with open(creds_data["service_account_file"]) as f:
            sa_info = json.load(f)
    credentials = service_account.Credentials.from_service_account_info(sa_info)
    return storage.Client(credentials=credentials), creds_data["bucket_name"]


def materialize():
    start = date.fromisoformat(os.environ["BRUIN_START_DATE"])
    end = date.fromisoformat(os.environ["BRUIN_END_DATE"])

    if os.environ.get("GCS_DATA_LAKE"):
        # Prod: load from GCS parquet files
        gcs, bucket_name = _gcs_client()
        bucket = gcs.bucket(bucket_name)
        frames = []
        for y, m in _iter_months(start, end):
            blob = bucket.blob(f"{GCS_PREFIX}/{y}/{m}.parquet")
            df = pd.read_parquet(BytesIO(blob.download_as_bytes()))
            print(f"Loaded {GCS_PREFIX}/{y}/{m}.parquet from GCS")
            frames.append(df)
        return pd.concat(frames, ignore_index=True)

    # Dev: download directly from CDS
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    frames = [_fetch_month_cds(y, m) for y, m in _iter_months(start, end)]
    return pd.concat(frames, ignore_index=True)
