/* @bruin
name: mart.monthly_global_trends
type: bq.sql

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
    primary_key: true
  - name: year
    type: INT64
  - name: month
    type: INT64
  - name: avg_sla_m
    type: FLOAT64
    description: Global average sea level anomaly in metres
  - name: avg_sst_celsius
    type: FLOAT64
    description: Global average sea surface temperature in °C
  - name: avg_sea_ice_fraction
    type: FLOAT64
    description: Global average sea ice fraction (0–1)
  - name: grid_points
    type: INT64
    description: Number of ocean grid points contributing to the averages

depends:
  - setup.init_mart_tables
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
FROM staging.sea_level_anomaly AS sla
INNER JOIN staging.sea_surface_temperature AS sst
    USING (year_month, latitude, longitude)
WHERE sla.year_month BETWEEN CAST('{{ start_date }}' AS DATE) AND CAST('{{ end_date }}' AS DATE)
GROUP BY 1, 2, 3
