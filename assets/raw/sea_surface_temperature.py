"""@bruin
name: raw.sea_surface_temperature
connection: duckdb-dev

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

secrets:
  - key: CDS_API_KEY
@bruin"""

import os
import tempfile
import warnings
import zipfile
from pathlib import Path

import cdsapi
import numpy as np
# import pandas as pd
import xarray as xr

warnings.filterwarnings("ignore")

DATASET = "satellite-sea-surface-temperature"

# Target grid: identical to the SLA 0.25° grid
TARGET_LAT = np.arange(-89.875, 90.0, 0.25)
TARGET_LON = np.arange(-179.875, 180.0, 0.25)


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]  # YYYY-MM-DD
    year = start_date[:4]
    month = start_date[5:7]

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        zip_path = tmp / "sst.zip"
        nc_dir = tmp / "nc"
        nc_dir.mkdir()

        client = cdsapi.Client(quiet=True)
        client.retrieve(
            DATASET,
            {
                "variable": "all",
                "version": "3_0",
                "processinglevel": "level_4",
                "sensor_on_satellite": "combined_product",
                "temporal_resolution": "monthly",
                "year": [year],
                "month": [month],
            },
            target=str(zip_path),
        )

        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(nc_dir)

        nc_files = sorted(nc_dir.glob("*.nc"))
        ds = xr.open_dataset(nc_files[0], engine="netcdf4")
        ds = ds[["analysed_sst", "sea_ice_fraction"]]

        # Align SST 0.05° grid onto SLA 0.25° grid via bilinear interpolation
        ds = ds.rename({"lat": "latitude", "lon": "longitude"})
        ds = ds.interp(latitude=TARGET_LAT, longitude=TARGET_LON, method="linear")

        # Kelvin → Celsius
        ds["analysed_sst"] = ds["analysed_sst"] - 273.15

        df = ds.to_dataframe().reset_index()
        df = df.dropna(subset=["analysed_sst"])
        df = df.rename(columns={"analysed_sst": "sst_celsius"})
        df["year"] = int(year)
        df["month"] = int(month)

        return df[["year", "month", "latitude", "longitude", "sst_celsius", "sea_ice_fraction"]]
