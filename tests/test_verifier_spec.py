"""The verifier's SQL on small, known inputs. The expected values here are the ones the spec
states, worked out by hand; none of them comes from the migration's code."""

import duckdb
import pytest

from verifier import spec


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'; INSTALL icu; LOAD icu;")
    c.execute(spec.MACROS)
    c.execute("""
        CREATE TABLE la_station (number INT, name VARCHAR, address VARCHAR,
            latitude DECIMAL(8,6), longitude DECIMAL(9,6), bike_stands SMALLINT);
        CREATE TABLE la_station_reading (reading_id BIGINT, station_number INT,
            reported_at TIMESTAMP, polled_at TIMESTAMP, bikes SMALLINT, free_stands SMALLINT,
            status VARCHAR, is_renting BOOLEAN, is_returning BOOLEAN);
        CREATE TABLE la_rental (rental_id BIGINT, from_station INT, to_station INT,
            started TIMESTAMP, ended TIMESTAMP);
        CREATE TABLE lb_asema (id VARCHAR, nimi VARCHAR, namn VARCHAR, name VARCHAR,
            osoite VARCHAR, adress VARCHAR, kaupunki VARCHAR, operaattori VARCHAR,
            kapasiteetti SMALLINT, x DECIMAL(16,13), y DECIMAL(16,13));
        CREATE TABLE lb_matka (id BIGINT, departure TIMESTAMP, "return" TIMESTAMP,
            departure_station_id VARCHAR, departure_station_name VARCHAR,
            return_station_id VARCHAR, return_station_name VARCHAR,
            covered_distance_m DECIMAL(10,2), duration_sec INT);
    """)
    return c


def build(c):
    c.execute(spec.TZ_HOURS)
    c.execute(spec.DUBLIN)
    c.execute(spec.HELSINKI)


def test_uuid5_in_sql_matches_the_value_checked_in_postgres(con):
    got = con.execute("SELECT CAST(uuid5('dublin:station:42') AS VARCHAR)").fetchone()[0]
    assert got == "926653f3-c058-5793-b863-554e4c585425"


def test_names_trim_ascii_whitespace_only(con):
    got = con.execute("SELECT clean('  Puotila '), clean(' '), clean(chr(160) || 'x')").fetchone()
    assert got == ("Puotila", None, "\xa0x")


def test_dublin_folds_and_the_spring_gap(con):
    con.execute("""
        INSERT INTO la_station VALUES (1, ' CLARENDON ROW', NULL, 53.340927, -6.262501, 31);
        INSERT INTO la_station_reading VALUES
          (10, 1, '2023-10-29 01:00:00', NULL, 3, 28, 'OPEN', NULL, NULL),
          (11, 1, '2023-10-29 01:30:00', NULL, 3, 28, 'OPEN', NULL, NULL),
          (12, 1, '2023-10-29 01:00:00', NULL, 3, 28, 'OPEN', NULL, NULL),
          (13, 1, '2023-10-29 01:30:00', NULL, 3, 28, 'OPEN', NULL, NULL),
          (14, 1, '2025-03-30 01:30:00', NULL, 3, 28, 'OPEN', NULL, NULL),
          (15, 1, '2025-07-01 13:00:00', NULL, 3, 28, 'CLOSED', NULL, NULL);
    """)
    build(con)
    rows = con.execute("""
        SELECT legacy_key, strftime(reported_at, '%H:%M'), quarantine_rule, is_open
        FROM exp_status ORDER BY legacy_key""").fetchall()
    assert rows == [
        ("10", "00:00", None, True),  # summer time: 01:00 IST is 00:00 UTC
        ("11", "00:30", None, True),
        ("12", "01:00", None, True),  # after the step back: winter time
        ("13", "01:30", None, True),
        ("14", None, "nonexistent_local_time", True),
        ("15", "12:00", None, False),
    ]
    assert con.execute("SELECT name FROM exp_station_dublin").fetchone()[0] == "CLARENDON ROW"


def test_helsinki_folds_by_duration_ties_and_stubs(con):
    con.execute("""
        INSERT INTO lb_asema VALUES ('043', 'Karhupuisto', NULL, NULL, NULL, NULL, ' ', ' ', 20,
            24.9526762998375, 60.1842832921200);
        INSERT INTO lb_matka VALUES
          (1, '2025-10-26 03:55:00', '2025-10-26 03:05:00', '043', NULL, 'A04', NULL, 900, 600),
          (2, '2025-10-26 03:30:00', '2025-10-26 03:40:00', '043', NULL, '043', NULL, -12, 600),
          (3, '2025-10-26 03:30:00', '2025-10-26 03:40:00', NULL, NULL, '043', NULL, 1941.67, NULL);
    """)
    build(con)
    rows = con.execute("""
        SELECT legacy_key, strftime(departed_at, '%H:%M'), strftime(returned_at, '%H:%M'),
               flags, distance_m
        FROM exp_trip_helsinki ORDER BY legacy_key""").fetchall()
    assert rows[0][:4] == ("1", "00:55", "01:05", [])
    assert rows[1][:4] == ("2", "00:30", "00:40", ["ambiguous_time", "negative_distance"])
    assert rows[1][4] is None
    assert rows[2][3] == ["ambiguous_time", "missing_departure_station"]
    assert str(rows[2][4]) == "1941.67"
    stations = {
        r[0]: r[1:]
        for r in con.execute(
            "SELECT source_ref, kind, name, city, latitude FROM exp_station_helsinki"
        ).fetchall()
    }
    assert stations["A04"][:2] == ("unlisted", "A04")  # a stub, named by its id
    assert stations["043"][2] is None and str(stations["043"][3]) == "60.184283"
