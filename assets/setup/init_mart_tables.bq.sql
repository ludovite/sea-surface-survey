/* @bruin
name: setup.init_mart_tables
type: bq.sql
connection: warehouse
@bruin */

CREATE TABLE IF NOT EXISTS staging.sea_surface_temperature (
    year_month       DATE,
    year             INT64,
    month            INT64,
    latitude         FLOAT64,
    longitude        FLOAT64,
    sst_celsius      FLOAT64,
    sea_ice_fraction FLOAT64
)
PARTITION BY year_month
CLUSTER BY year, month;

CREATE TABLE IF NOT EXISTS mart.monthly_global_trends (
    year_month           DATE,
    year                 INT64,
    month                INT64,
    avg_sla_m            FLOAT64,
    avg_sst_celsius      FLOAT64,
    avg_sea_ice_fraction FLOAT64,
    grid_points          INT64
)
PARTITION BY year_month
CLUSTER BY year, month;

CREATE TABLE IF NOT EXISTS mart.latitude_zone_stats (
    year_month           DATE,
    year                 INT64,
    month                INT64,
    latitude_zone        STRING,
    avg_sla_m            FLOAT64,
    avg_sst_celsius      FLOAT64,
    avg_sea_ice_fraction FLOAT64,
    grid_points          INT64
)
PARTITION BY year_month
CLUSTER BY latitude_zone;
