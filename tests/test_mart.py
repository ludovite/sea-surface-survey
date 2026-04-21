"""Tests for mart query logic and invariants."""


GLOBAL_TRENDS_SQL = """
    SELECT
        sla.year_month,
        sla.year,
        sla.month,
        AVG(sla.sea_level_anomaly_m) AS avg_sla_m,
        AVG(sst.sst_celsius)         AS avg_sst_celsius,
        AVG(sst.sea_ice_fraction)    AS avg_sea_ice_fraction,
        COUNT(*)                     AS grid_points
    FROM staging_sla AS sla
    INNER JOIN staging_sst AS sst
        USING (year_month, latitude, longitude)
    GROUP BY 1, 2, 3
"""

ZONE_STATS_SQL = """
    SELECT
        sla.year_month,
        CASE
            WHEN sla.latitude >=  66.5 THEN 'Arctic'
            WHEN sla.latitude >=  23.5 THEN 'N. Temperate'
            WHEN sla.latitude >= -23.5 THEN 'Tropical'
            WHEN sla.latitude >= -66.5 THEN 'S. Temperate'
            ELSE                             'Antarctic'
        END                              AS latitude_zone,
        AVG(sla.sea_level_anomaly_m)     AS avg_sla_m,
        AVG(sst.sst_celsius)             AS avg_sst_celsius,
        COUNT(*)                         AS grid_points
    FROM staging_sla AS sla
    INNER JOIN staging_sst AS sst
        USING (year_month, latitude, longitude)
    GROUP BY 1, 2
"""


def test_global_trends_join_produces_rows(conn):
    """Aligned SLA and SST staging data produces at least one result row."""
    rows = conn.execute(GLOBAL_TRENDS_SQL).fetchall()
    assert len(rows) > 0


def test_global_trends_grid_points_positive(conn):
    """grid_points must be > 0 for every result row."""
    rows = conn.execute(f"SELECT grid_points FROM ({GLOBAL_TRENDS_SQL})").fetchall()
    assert all(r[0] > 0 for r in rows)


def test_global_trends_no_null_year_month(conn):
    """year_month must never be NULL in monthly_global_trends."""
    nulls = conn.execute(
        f"SELECT COUNT(*) FROM ({GLOBAL_TRENDS_SQL}) WHERE year_month IS NULL"
    ).fetchone()[0]
    assert nulls == 0


def test_global_trends_year_month_unique(conn):
    """Each year_month appears exactly once in monthly_global_trends."""
    result = conn.execute(f"""
        SELECT COUNT(*) = COUNT(DISTINCT year_month)
        FROM ({GLOBAL_TRENDS_SQL})
    """).fetchone()[0]
    assert result is True


def test_zone_stats_valid_zone_names(conn):
    """All latitude_zone values belong to the five expected zones."""
    expected = {"Arctic", "N. Temperate", "Tropical", "S. Temperate", "Antarctic"}
    rows = conn.execute(
        f"SELECT DISTINCT latitude_zone FROM ({ZONE_STATS_SQL})"
    ).fetchall()
    zones = {r[0] for r in rows}
    assert zones.issubset(expected)


def test_misaligned_data_produces_no_join(conn):
    """SLA and SST at different coordinates → INNER JOIN returns 0 rows."""
    conn.execute("""
        CREATE TEMP TABLE sla_offset AS SELECT * FROM (VALUES
            (DATE '1993-02-01', 1993, 2, 5.0, 15.0, 0.03)
        ) t(year_month, year, month, latitude, longitude, sea_level_anomaly_m)
    """)
    rows = conn.execute("""
        SELECT COUNT(*) FROM sla_offset AS sla
        INNER JOIN staging_sst AS sst USING (year_month, latitude, longitude)
    """).fetchone()[0]
    assert rows == 0
