/* @bruin
name: staging.sea_level_anomaly
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
  - name: sea_level_anomaly_m
    type: FLOAT
    description: Sea level anomaly in metres

depends:
  - raw.sea_level_anomaly
@bruin */

SELECT
    MAKE_DATE(year, month, 1)                           AS year_month,
    year,
    month,
    latitude,
    longitude,
    sea_level_anomaly_m
FROM "sea-survey".raw.sea_level_anomaly
WHERE
    year  = EXTRACT(year  FROM CAST('{{ start_date }}' AS DATE))
    AND month = EXTRACT(month FROM CAST('{{ start_date }}' AS DATE))
