"""@bruin
name: raw.sea_level_anomaly
image: python:3.12
connection: gcp-prod

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
from pathlib import Path

import cdsapi
import pandas as pd
import xarray as xr

warnings.filterwarnings("ignore")

DATASET = "satellite-sea-level-global"


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]  # YYYY-MM-DD
    year = start_date[:4]
    month = start_date[5:7]

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        zip_path = tmp / "sla.zip"
        nc_dir = tmp / "nc"
        nc_dir.mkdir()

        client = cdsapi.Client(quiet=True)
        client.retrieve(
            DATASET,
            {
                "variable": "monthly_mean",
                "year": [year],
                "month": [month],
                "version": "vdt2024",
            },
            target=str(zip_path),
        )

        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(nc_dir)

        ds = xr.open_mfdataset(sorted(nc_dir.glob("*.nc")), combine="by_coords")

        df = ds["sla"].to_dataframe().reset_index()
        df = df.dropna(subset=["sla"])
        df = df.rename(columns={"sla": "sea_level_anomaly_m"})
        df["year"] = int(year)
        df["month"] = int(month)

        return df[["year", "month", "latitude", "longitude", "sea_level_anomaly_m"]]
