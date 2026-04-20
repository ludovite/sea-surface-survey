/* @bruin
name: mart.latitude_zone_stats
type: bq.sql

description: >
  Monthly averages of sea level anomaly and sea surface temperature
  aggregated by latitude zone (Arctic, N. Temperate, Tropical, S. Temperate, Antarctic).
  Used for the categorical distribution dashboard tile.

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
  - name: latitude_zone
    primary_key: true
    type: STRING
    description: "Latitude zone: Arctic (≥66.5°), N. Temperate (23.5–66.5°), Tropical (±23.5°), S. Temperate (-66.5 to -23.5°), Antarctic (≤-66.5°)"
  - name: avg_sla_m
    type: FLOAT64
    description: Average sea level anomaly in metres for the zone
  - name: avg_sst_celsius
    type: FLOAT64
    description: Average sea surface temperature in °C for the zone
  - name: avg_sea_ice_fraction
    type: FLOAT64
    description: Average sea ice fraction (0–1) for the zone
  - name: grid_points
    type: INT64
    description: Number of ocean grid points in the zone

depends:
  - setup.init_mart_tables
  - staging.sea_level_anomaly
  - staging.sea_surface_temperature
@bruin */

SELECT
    sla.year_month,
    sla.year,
    sla.month,
    CASE
        WHEN sla.latitude >=  66.5 THEN 'Arctic'
        WHEN sla.latitude >=  23.5 THEN 'N. Temperate'
        WHEN sla.latitude >= -23.5 THEN 'Tropical'
        WHEN sla.latitude >= -66.5 THEN 'S. Temperate'
        ELSE                             'Antarctic'
    END                               AS latitude_zone,
    AVG(sla.sea_level_anomaly_m)      AS avg_sla_m,
    AVG(sst.sst_celsius)              AS avg_sst_celsius,
    AVG(sst.sea_ice_fraction)         AS avg_sea_ice_fraction,
    COUNT(*)                          AS grid_points
FROM staging.sea_level_anomaly AS sla
INNER JOIN staging.sea_surface_temperature AS sst
    USING (year_month, latitude, longitude)
WHERE sla.year_month BETWEEN CAST('{{ start_date }}' AS DATE) AND CAST('{{ end_date }}' AS DATE)
GROUP BY 1, 2, 3, 4
