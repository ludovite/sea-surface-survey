/* @bruin
name: staging.sea_level_anomaly
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
  - name: sea_level_anomaly_m
    type: FLOAT
    description: Sea level anomaly in metres

depends:
  - raw.sea_level_anomaly
@bruin */

SELECT
    DATE(year, month, 1)    AS year_month,
    year,
    month,
    latitude,
    longitude,
    sea_level_anomaly_m
FROM raw.sea_level_anomaly
WHERE
    year  = EXTRACT(year  FROM DATE('{{ start_date }}'))
    AND month = EXTRACT(month FROM DATE('{{ start_date }}'))
