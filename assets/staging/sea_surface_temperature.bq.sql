/* @bruin
name: staging.sea_surface_temperature
type: bq.sql
connection: warehouse

materialization:
  type: view

columns:
  - name: year_month
    type: DATE
    description: First day of the observation month
  - name: year
    type: INT64
  - name: month
    type: INT64
  - name: latitude
    type: FLOAT64
    description: Latitude, 0.25° grid
  - name: longitude
    type: FLOAT64
    description: Longitude, 0.25° grid
  - name: sst_celsius
    type: FLOAT64
    description: Sea surface temperature in °C
  - name: sea_ice_fraction
    type: FLOAT64
    description: Sea ice fraction (0–1)

depends:
  - raw.sea_surface_temperature
@bruin */

SELECT
    DATE(year, month, 1)     AS year_month,
    year,
    month,
    latitude,
    longitude,
    sst_celsius,
    sea_ice_fraction
FROM raw.sea_surface_temperature
