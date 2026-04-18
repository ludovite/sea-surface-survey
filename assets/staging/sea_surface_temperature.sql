/* @bruin
name: staging.sea_surface_temperature
type: bq.sql

materialization:
  type: table
  strategy: delete+insert
  incremental_key: year_month
  partition_by: year_month
  cluster_by:
    - latitude
    - longitude

columns:
  - name: year_month
    type: DATE
    description: First day of the observation month (partition key)
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
    DATE(year, month, 1)    AS year_month,
    year,
    month,
    latitude,
    longitude,
    sst_celsius,
    sea_ice_fraction
FROM raw.sea_surface_temperature
WHERE
    year  = EXTRACT(year  FROM DATE('{{ start_date }}'))
    AND month = EXTRACT(month FROM DATE('{{ start_date }}'))
