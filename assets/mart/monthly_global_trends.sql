/* @bruin
name: mart.monthly_global_trends
type: duckdb.sql

description: >
  Global monthly averages of sea level anomaly and sea surface temperature.
  Used for the temporal distribution dashboard tile.

materialization:
  type: table
  strategy: delete+insert
  incremental_key: year_month

columns:
  - name: year_month
    type: DATE
    description: First day of the observation month
  - name: year
    type: INTEGER
  - name: month
    type: INTEGER
  - name: avg_sla_m
    type: FLOAT
    description: Global average sea level anomaly in metres
  - name: avg_sst_celsius
    type: FLOAT
    description: Global average sea surface temperature in °C
  - name: avg_sea_ice_fraction
    type: FLOAT
    description: Global average sea ice fraction (0–1)
  - name: grid_points
    type: INTEGER
    description: Number of ocean grid points contributing to the averages

depends:
  - staging.sea_level_anomaly
  - staging.sea_surface_temperature
@bruin */

SELECT
    sla.year_month,
    sla.year,
    sla.month,
    AVG(sla.sea_level_anomaly_m)  AS avg_sla_m,
    AVG(sst.sst_celsius)          AS avg_sst_celsius,
    AVG(sst.sea_ice_fraction)     AS avg_sea_ice_fraction,
    COUNT(*)                      AS grid_points
FROM "sea-survey".staging.sea_level_anomaly AS sla
INNER JOIN "sea-survey".staging.sea_surface_temperature AS sst
    USING (year_month, latitude, longitude)
WHERE sla.year_month = CAST('{{ start_date }}' AS DATE)
GROUP BY
    sla.year_month,
    sla.year,
    sla.month
