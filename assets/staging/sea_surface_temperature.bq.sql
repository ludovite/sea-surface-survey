/* @bruin
name: staging.sea_surface_temperature
type: bq.sql
connection: warehouse

materialization:
  type: table
  strategy: delete+insert
  incremental_key: year_month

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
    description: Latitude, regridded to 0.25° grid
  - name: longitude
    type: FLOAT64
    description: Longitude, regridded to 0.25° grid
  - name: sst_celsius
    type: FLOAT64
    description: Sea surface temperature in °C, averaged over 0.25° cell
  - name: sea_ice_fraction
    type: FLOAT64
    description: Sea ice fraction (0–1), averaged over 0.25° cell

depends:
  - setup.init_mart_tables
  - raw.sea_surface_temperature
@bruin */

SELECT
    DATE(CAST(year AS INT64), CAST(month AS INT64), 1) AS year_month,
    CAST(year AS INT64)                                AS year,
    CAST(month AS INT64)                               AS month,
    ROUND(CAST(latitude  AS FLOAT64) / 0.25) * 0.25 AS latitude,
    ROUND(CAST(longitude AS FLOAT64) / 0.25) * 0.25 AS longitude,
    AVG(sst_celsius)                            AS sst_celsius,
    AVG(sea_ice_fraction)                       AS sea_ice_fraction
FROM raw.sea_surface_temperature
GROUP BY 1, 2, 3, 4, 5
