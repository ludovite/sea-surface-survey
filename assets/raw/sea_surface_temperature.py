"""@bruin
name: raw.sea_surface_temperature_to_gcs
tags:
  - ingestion

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

import cdsapi
import pandas as pd
import xarray as xr

warnings.filterwarnings("ignore")

DATASET = "satellite-sea-surface-temperature"
CACHE_DIR = Path(__file__).parents[2] / "data" / "cache" / "sst"
GCS_PREFIX = "raw/sea_surface_temperature"


def _iter_months(start: date, end: date):
    current = date(start.year, start.month, 1)
    stop = date(end.year, end.month, 1)
    while current <= stop:
        yield current.year, f"{current.month:02d}"
        month = current.month % 12 + 1
        year = current.year + (current.month == 12)
        current = date(year, month, 1)


def _fetch_month(year: int, month: str) -> pd.DataFrame:
    cached_zip = CACHE_DIR / f"{year}_{month}.zip"

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        nc_dir = tmp / "nc"
        nc_dir.mkdir()

        if not cached_zip.exists():
            client = cdsapi.Client(quiet=True, key=os.environ["CDS_API_KEY"], url="https://cds.climate.copernicus.eu/api")
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


start = date.fromisoformat(os.environ["BRUIN_START_DATE"])
end = date.fromisoformat(os.environ["BRUIN_END_DATE"])

if not os.environ.get("GCS_DATA_LAKE"):
    print("No GCS credentials — skipping upload (dev environment)")
else:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    client, bucket_name = _gcs_client()
    bucket = client.bucket(bucket_name)

    for y, m in _iter_months(start, end):
        df = _fetch_month(y, m)
        blob = bucket.blob(f"{GCS_PREFIX}/{y}/{m}.parquet")
        blob.upload_from_string(df.to_parquet(index=False), content_type="application/octet-stream")
        print(f"Uploaded {GCS_PREFIX}/{y}/{m}.parquet to GCS")
