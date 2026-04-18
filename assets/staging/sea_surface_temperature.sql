/* @bruin
name: staging.sea_surface_temperature
type: duckdb.sql

materialization:
  type: table
  strategy: create+replace

columns:
  - name: year_month
    type: DATE
    description: First day of the observation month
  - name: year
    type: INTEGER
  - name: month
    type: INTEGER
  - name: latitude
    type: FLOAT
    description: Latitude, 0.25° grid
  - name: longitude
    type: FLOAT
    description: Longitude, 0.25° grid
  - name: sst_celsius
    type: FLOAT
    description: Sea surface temperature in °C
  - name: sea_ice_fraction
    type: FLOAT
    description: Sea ice fraction (0–1)

depends:
  - raw.sea_surface_temperature
@bruin */

SELECT
    MAKE_DATE(year, month, 1)                           AS year_month,
    year,
    month,
    latitude,
    longitude,
    sst_celsius,
    sea_ice_fraction
FROM raw.sea_surface_temperature
WHERE
    year  = EXTRACT(year  FROM CAST('{{ start_date }}' AS DATE))
    AND month = EXTRACT(month FROM CAST('{{ start_date }}' AS DATE))
