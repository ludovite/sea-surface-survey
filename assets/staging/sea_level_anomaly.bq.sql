/* @bruin
name: staging.sea_level_anomaly
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
  - name: sea_level_anomaly_m
    type: FLOAT64
    description: Sea level anomaly in metres

depends:
  - raw.sea_level_anomaly
@bruin */

SELECT
    DATE(year, month, 1)                             AS year_month,
    year,
    month,
    ROUND(CAST(latitude  AS FLOAT64) / 0.25) * 0.25 AS latitude,
    ROUND(CAST(longitude AS FLOAT64) / 0.25) * 0.25 AS longitude,
    AVG(sea_level_anomaly_m)                         AS sea_level_anomaly_m
FROM raw.sea_level_anomaly
GROUP BY 1, 2, 3, 4, 5
