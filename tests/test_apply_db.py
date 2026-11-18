"""apply_batch against a real PostgreSQL target, in a throwaway database.

Needs `docker compose up -d target`; skipped when it is not reachable.
"""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from migration import transform as tf
from migration.apply import Change, apply_batch

ROOT = Path(__file__).resolve().parents[1]
ADMIN = "host=localhost port=5434 dbname=fleet user=fleet password=fleet"
TEST = "host=localhost port=5434 dbname=fleet_test user=fleet password=fleet"


def _reachable() -> bool:
    try:
        psycopg.connect(ADMIN, connect_timeout=2).close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _reachable(), reason="target not running")


@pytest.fixture
def db():
    with psycopg.connect(ADMIN, autocommit=True) as conn:
        conn.execute("DROP DATABASE IF EXISTS fleet_test WITH (FORCE)")
        conn.execute("CREATE DATABASE fleet_test")
    with psycopg.connect(TEST, autocommit=True) as conn:
        conn.execute((ROOT / "docker" / "target" / "001_schema.sql").read_text(encoding="utf8"))
    conn = psycopg.connect(TEST)
    yield conn
    conn.close()


def trip_row(trip_key, dep, ret, distance="1000"):
    return tf.helsinki_trip(
        {
            "id": trip_key,
            "departure": datetime(2025, 7, 1, 10, 0),
            "return": datetime(2025, 7, 1, 10, 10),
            "departure_station_id": dep,
            "return_station_id": ret,
            "covered_distance_m": distance,
            "duration_sec": 600,
        }
    )


def trip_change(key, pos, kind="upsert", dep="043", ret="A04", distance="1000", old=()):
    row = trip_row(key, dep, ret, distance) if kind == "upsert" else None
    return Change(
        "trip",
        tf.target_id("helsinki", "matka", key),
        pos,
        kind,
        row,
        ("helsinki", "matka", str(key)),
        None,
        old,
        tuple(s for s in (dep, ret) if s) if kind == "upsert" else (),
    )


def scalar(conn, sql, *args):
    return conn.execute(sql, args).fetchone()[0]


def test_replays_and_old_changes_are_no_ops(db):
    apply_batch(db, [trip_change(1, (10, 0), distance="1000")], {})
    stats = apply_batch(
        db, [trip_change(1, (10, 0), distance="1000"), trip_change(1, (5, 0), distance="9")], {}
    )
    assert stats["stale"] == 1
    assert scalar(db, "SELECT distance_m FROM fleet.trip") == Decimal("1000.00")
    apply_batch(db, [trip_change(1, (11, 0), distance="1200.50")], {})
    assert scalar(db, "SELECT distance_m FROM fleet.trip") == Decimal("1200.50")


def test_a_delete_leaves_a_tombstone_that_refuses_a_late_insert(db):
    apply_batch(db, [trip_change(1, (10, 0))], {})
    apply_batch(db, [trip_change(1, (12, 0), "delete", old=("043", "A04"))], {})
    assert scalar(db, "SELECT count(*) FROM fleet.trip") == 0
    stats = apply_batch(db, [trip_change(1, (11, 0))], {})  # older than the delete
    assert stats["stale"] == 1 and scalar(db, "SELECT count(*) FROM fleet.trip") == 0
    assert scalar(db, "SELECT deleted FROM migration.row_version") is True


def test_stubs_appear_with_the_first_trip_and_go_with_the_last(db):
    apply_batch(db, [trip_change(1, (10, 0)), trip_change(2, (10, 1))], {})
    assert scalar(db, "SELECT count(*) FROM fleet.station WHERE kind = 'unlisted'") == 2
    apply_batch(db, [trip_change(1, (11, 0), "delete", old=("043", "A04"))], {})
    assert scalar(db, "SELECT count(*) FROM fleet.station WHERE kind = 'unlisted'") == 2
    apply_batch(db, [trip_change(2, (12, 0), "delete", old=("043", "A04"))], {})
    assert scalar(db, "SELECT count(*) FROM fleet.station") == 0


def test_a_listed_station_replaces_its_stub_and_returns_to_one_when_deleted(db):
    apply_batch(db, [trip_change(1, (10, 0))], {})
    asema = tf.helsinki_station(
        {
            "id": "043",
            "nimi": "Karhupuisto",
            "namn": None,
            "name": None,
            "osoite": None,
            "kaupunki": " ",
            "kapasiteetti": 20,
            "x": "24.9526762998375",
            "y": "60.1842832921200",
        }
    )
    sid = tf.target_id("helsinki", "asema", "043")
    station = Change("station", sid, (11, 0), "upsert", asema, ("helsinki", "asema", "043"))
    apply_batch(db, [station], {})
    assert db.execute(
        "SELECT kind, name, latitude FROM fleet.station WHERE station_id = %s", (sid,)
    ).fetchone() == ("public", "Karhupuisto", Decimal("60.184283"))
    gone = Change("station", sid, (12, 0), "delete", None, ("helsinki", "asema", "043"))
    apply_batch(db, [gone], {})
    assert db.execute(
        "SELECT kind, name FROM fleet.station WHERE station_id = %s", (sid,)
    ).fetchone() == ("unlisted", "043")


def test_quarantine_in_and_out(db):
    apply_batch(db, [trip_change(1, (10, 0))], {})
    bad = Change(
        "trip",
        tf.target_id("helsinki", "matka", 1),
        (11, 0),
        "quarantine",
        None,
        ("helsinki", "matka", "1"),
        ("nonexistent_local_time", "", {"id": 1}),
        ("043", "A04"),
    )
    apply_batch(db, [bad], {})
    assert scalar(db, "SELECT count(*) FROM fleet.trip") == 0
    assert scalar(db, "SELECT rule FROM migration.quarantine") == "nonexistent_local_time"
    apply_batch(db, [trip_change(1, (12, 0))], {})
    assert scalar(db, "SELECT count(*) FROM fleet.trip") == 1
    assert scalar(db, "SELECT count(*) FROM migration.quarantine") == 0


def test_offsets_and_transactions_commit_with_the_rows(db):
    apply_batch(
        db,
        [trip_change(1, (10, 0))],
        {("helsinki.helsinki.matka", 0): 42},
        ("helsinki", ["gtid:1"]),
    )
    assert scalar(db, "SELECT next_offset FROM migration.consumer_offset") == 42
    assert scalar(db, "SELECT txn_id FROM migration.applied_txn") == "gtid:1"
    assert scalar(db, "SELECT departed_at FROM fleet.trip") == datetime(
        2025, 7, 1, 7, 0, tzinfo=UTC
    )
