import duckdb
import pytest


@pytest.fixture
def conn():
    """In-memory DuckDB connection with synthetic staging data."""
    c = duckdb.connect()

    # Raw SST: 4 points at 0.05° resolution inside the same 0.25° cell (lat=0, lon=10)
    # plus one Arctic point
    c.execute("""
        CREATE TABLE raw_sst AS SELECT * FROM (VALUES
            (1993, 1,  0.00, 10.00, 25.0, 0.00),
            (1993, 1,  0.05, 10.00, 27.0, 0.00),
            (1993, 1,  0.10, 10.05, 26.0, 0.00),
            (1993, 1,  0.12, 10.10, 28.0, 0.00),
            (1993, 1, 70.00, 20.00, -1.5, 0.80)
        ) t(year, month, latitude, longitude, sst_celsius, sea_ice_fraction)
    """)

    # Raw SLA: already on 0.25° grid
    c.execute("""
        CREATE TABLE raw_sla AS SELECT * FROM (VALUES
            (1993, 1,  0.00, 10.00, 0.05),
            (1993, 1, 70.00, 20.00, 0.12)
        ) t(year, month, latitude, longitude, sea_level_anomaly_m)
    """)

    # staging_sst: regrid to 0.25° via ROUND + AVG
    c.execute("""
        CREATE VIEW staging_sst AS
        SELECT
            MAKE_DATE(year, month, 1)             AS year_month,
            year,
            month,
            ROUND(latitude  / 0.25) * 0.25        AS latitude,
            ROUND(longitude / 0.25) * 0.25        AS longitude,
            AVG(sst_celsius)                       AS sst_celsius,
            AVG(sea_ice_fraction)                  AS sea_ice_fraction
        FROM raw_sst
        GROUP BY 1, 2, 3, 4, 5
    """)

    # staging_sla: normalise grid (SLA already at 0.25°, ROUND is a no-op)
    c.execute("""
        CREATE VIEW staging_sla AS
        SELECT
            MAKE_DATE(year, month, 1)             AS year_month,
            year,
            month,
            ROUND(latitude  / 0.25) * 0.25        AS latitude,
            ROUND(longitude / 0.25) * 0.25        AS longitude,
            AVG(sea_level_anomaly_m)               AS sea_level_anomaly_m
        FROM raw_sla
        GROUP BY 1, 2, 3, 4, 5
    """)

    return c
