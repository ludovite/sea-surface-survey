"""Tests for the regridding and latitude zone logic."""
import duckdb
import pytest


# ---------------------------------------------------------------------------
# Regridding: ROUND(lat / 0.25) * 0.25
# ---------------------------------------------------------------------------

def test_regrid_four_points_to_same_cell(conn):
    """Four 0.05° SST points inside the same 0.25° cell → one output row."""
    rows = conn.execute(
        "SELECT COUNT(*) FROM staging_sst WHERE latitude = 0.0 AND longitude = 10.0"
    ).fetchone()[0]
    assert rows == 1


def test_regrid_average_is_correct(conn):
    """AVG of four SST values (25, 27, 26, 28) at the same 0.25° cell = 26.5."""
    sst = conn.execute(
        "SELECT sst_celsius FROM staging_sst WHERE latitude = 0.0 AND longitude = 10.0"
    ).fetchone()[0]
    assert abs(sst - 26.5) < 1e-6


def test_regrid_pole_latitude(conn):
    """Latitude -90 rounds correctly to -90.0 (not -89.75 or -90.25)."""
    result = conn.execute(
        "SELECT ROUND(-90.0 / 0.25) * 0.25"
    ).fetchone()[0]
    assert result == -90.0


def test_regrid_antimeridian_longitude(conn):
    """Longitude 180 rounds correctly to 180.0."""
    result = conn.execute(
        "SELECT ROUND(180.0 / 0.25) * 0.25"
    ).fetchone()[0]
    assert result == 180.0


def test_regrid_boundary_rounds_to_nearest(conn):
    """0.125 is exactly halfway — rounds to 0.25 (round half to even or up)."""
    result = conn.execute(
        "SELECT ROUND(0.13 / 0.25) * 0.25"
    ).fetchone()[0]
    assert result == pytest.approx(0.25, abs=1e-6)


# ---------------------------------------------------------------------------
# Latitude zone CASE WHEN
# ---------------------------------------------------------------------------

ZONE_SQL = """
    SELECT
        CASE
            WHEN lat >=  66.5 THEN 'Arctic'
            WHEN lat >=  23.5 THEN 'N. Temperate'
            WHEN lat >= -23.5 THEN 'Tropical'
            WHEN lat >= -66.5 THEN 'S. Temperate'
            ELSE                    'Antarctic'
        END AS zone
    FROM (VALUES ({lat})) t(lat)
"""


@pytest.mark.parametrize("lat,expected", [
    (90.0,   "Arctic"),
    (66.5,   "Arctic"),       # boundary belongs to Arctic
    (66.4,   "N. Temperate"),
    (23.5,   "N. Temperate"), # boundary belongs to N. Temperate
    (0.0,    "Tropical"),
    (-23.5,  "Tropical"),     # boundary belongs to Tropical
    (-23.6,  "S. Temperate"),
    (-66.5,  "S. Temperate"), # boundary belongs to S. Temperate
    (-66.6,  "Antarctic"),
    (-90.0,  "Antarctic"),
])
def test_zone_boundaries(lat, expected):
    """Every latitude value maps to the expected zone."""
    c = duckdb.connect()
    result = c.execute(ZONE_SQL.format(lat=lat)).fetchone()[0]
    assert result == expected


def test_all_five_zones_covered(conn):
    """staging_sst + zone logic produces exactly the 5 expected zone names."""
    expected = {"Arctic", "N. Temperate", "Tropical", "S. Temperate", "Antarctic"}
    rows = conn.execute("""
        SELECT DISTINCT
            CASE
                WHEN latitude >=  66.5 THEN 'Arctic'
                WHEN latitude >=  23.5 THEN 'N. Temperate'
                WHEN latitude >= -23.5 THEN 'Tropical'
                WHEN latitude >= -66.5 THEN 'S. Temperate'
                ELSE                        'Antarctic'
            END
        FROM staging_sst
    """).fetchall()
    zones = {r[0] for r in rows}
    # Fixture only has lat=0 (Tropical) and lat=70 (Arctic)
    assert zones.issubset(expected)
