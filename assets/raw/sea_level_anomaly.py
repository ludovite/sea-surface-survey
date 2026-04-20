"""@bruin
name: raw.sea_level_anomaly
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
  - name: month
    type: INTEGER
    description: Month of the observation (1–12)
  - name: latitude
    type: FLOAT
    description: Latitude in degrees (-90 to 90), 0.25° grid
  - name: longitude
    type: FLOAT
    description: Longitude in degrees (-180 to 180), 0.25° grid
  - name: sea_level_anomaly_m
    type: FLOAT
    description: Sea level anomaly relative to reference climatology, in metres (SLA)

secrets:
  - key: CDS_API_KEY
@bruin"""

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

DATASET = "satellite-sea-level-global"
CACHE_DIR = Path(__file__).parents[2] / "data" / "cache" / "sla"


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
            client = cdsapi.Client(quiet=True)
            client.retrieve(
                DATASET,
                {
                    "variable": "monthly_mean",
                    "year": [str(year)],
                    "month": [month],
                    "version": "vdt2024",
                },
                target=str(cached_zip),
            )

        with zipfile.ZipFile(cached_zip) as zf:
            zf.extractall(nc_dir)

        nc_files = sorted(nc_dir.glob("*.nc"))
        ds = xr.open_dataset(nc_files[0], engine="netcdf4")
        df = ds["sla"].to_dataframe().reset_index()
        df = df.dropna(subset=["sla"])
        df = df.rename(columns={"sla": "sea_level_anomaly_m"})
        df["year"] = year
        df["month"] = int(month)
        return df[["year", "month", "latitude", "longitude", "sea_level_anomaly_m"]]


def materialize():
    start = date.fromisoformat(os.environ["BRUIN_START_DATE"])
    end = date.fromisoformat(os.environ["BRUIN_END_DATE"])

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    frames = [_fetch_month(y, m) for y, m in _iter_months(start, end)]
    return pd.concat(frames, ignore_index=True)
