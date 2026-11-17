"""The legacy APIs against real PostgreSQL and MySQL, in throwaway databases.

Needs `docker compose up -d legacy-a legacy-b`; skipped when they are not reachable.
"""

import asyncio
import uuid
from pathlib import Path

import httpx
import psycopg
import pymysql
import pytest

ROOT = Path(__file__).resolve().parents[1]
PG_ADMIN = "host=localhost port=5433 dbname=dublin user=dublin password=dublin"
PG_TEST = "host=localhost port=5433 dbname=dublin_test user=dublin password=dublin"


def _reachable() -> bool:
    try:
        psycopg.connect(PG_ADMIN, connect_timeout=2).close()
        pymysql.connect(host="127.0.0.1", port=3307, user="root", password="root").close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _reachable(), reason="legacy databases not running")


def _sql(name: str, system: str) -> str:
    return (ROOT / "docker" / system / name).read_text(encoding="utf8")


@pytest.fixture(scope="module")
def dublin_db():
    with psycopg.connect(PG_ADMIN, autocommit=True) as conn:
        conn.execute("DROP DATABASE IF EXISTS dublin_test WITH (FORCE)")
        conn.execute("CREATE DATABASE dublin_test")
    with psycopg.connect(PG_TEST, autocommit=True) as conn:
        conn.execute(_sql("001_schema.sql", "legacy_a"))
        conn.execute(_sql("002_api.sql", "legacy_a"))
        conn.execute(
            "INSERT INTO station VALUES (1, 'CLARENDON ROW', 'Clarendon Row', 53.340927, "
            "-6.262501, 31), (2, 'BLESSINGTON STREET', NULL, 53.3568, -6.26814, 20)"
        )
        conn.execute(
            "INSERT INTO station_reading (station_number, reported_at, bikes, free_stands, status) "
            "VALUES (1, '2026-11-17 09:00', 1, 30, 'OPEN'), (2, '2026-11-17 09:00', 19, 1, 'OPEN')"
        )
    yield PG_TEST


@pytest.fixture(scope="module")
def helsinki_db():
    root = pymysql.connect(
        host="127.0.0.1",
        port=3307,
        user="root",
        password="root",
        autocommit=True,
        client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
    )
    with root.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS helsinki_test")
        cur.execute("CREATE DATABASE helsinki_test")
        cur.execute("USE helsinki_test")
        schema = _sql("001_schema.sql", "legacy_b").split("-- The load runs")[0]
        cur.execute(schema)
        cur.execute(_sql("002_api.sql", "legacy_b"))
        cur.execute(
            "INSERT INTO asema (id, nimi, kapasiteetti) VALUES "
            "('043', 'Karhupuisto', 20), ('A04', 'Eiran Sairaala', 12)"
        )
    root.close()
    yield "helsinki_test"


async def _client(module):
    transport = httpx.ASGITransport(app=module.app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


def _run(coro):
    # psycopg's async mode cannot use Windows' default Proactor loop; the APIs themselves run on
    # Linux in their containers, where this does not arise.
    return asyncio.run(coro, loop_factory=asyncio.SelectorEventLoop)


def test_dublin_rental_writes_two_tables_in_one_transaction_and_replays(dublin_db, monkeypatch):
    monkeypatch.setenv("LEGACY_A_DSN", dublin_db)
    from legacy_api import dublin

    async def scenario():
        async with dublin.app.router.lifespan_context(dublin.app), await _client(dublin) as c:
            key = str(uuid.uuid4())
            first = await c.post(
                "/rentals", json={"from_station": 1}, headers={"Idempotency-Key": key}
            )
            again = await c.post(
                "/rentals", json={"from_station": 1}, headers={"Idempotency-Key": key}
            )
            other_body = await c.post(
                "/rentals", json={"from_station": 2}, headers={"Idempotency-Key": key}
            )
            empty = await c.post(
                "/rentals", json={"from_station": 1}, headers={"Idempotency-Key": str(uuid.uuid4())}
            )
            no_key = await c.post("/rentals", json={"from_station": 1})
            return first, again, other_body, empty, no_key

    first, again, other_body, empty, no_key = _run(scenario())
    assert first.status_code == 200 and again.json() == first.json()
    assert other_body.status_code == 422
    assert empty.status_code == 409  # the one bike is gone
    assert no_key.status_code == 400
    with psycopg.connect(dublin_db) as conn:
        assert conn.execute("SELECT count(*) FROM rental").fetchone()[0] == 1
        bikes, free = conn.execute(
            "SELECT bikes, free_stands FROM station_reading WHERE station_number = 1 "
            "ORDER BY reading_id DESC LIMIT 1"
        ).fetchone()
        assert (bikes, free) == (0, 31)


def test_dublin_concurrent_retries_of_one_key_write_once(dublin_db, monkeypatch):
    monkeypatch.setenv("LEGACY_A_DSN", dublin_db)
    from legacy_api import dublin

    async def scenario():
        async with dublin.app.router.lifespan_context(dublin.app), await _client(dublin) as c:
            key = str(uuid.uuid4())
            return await asyncio.gather(
                *[
                    c.post("/rentals", json={"from_station": 2}, headers={"Idempotency-Key": key})
                    for _ in range(8)
                ]
            )

    responses = _run(scenario())
    assert {r.status_code for r in responses} == {200}
    assert len({r.json()["rental_id"] for r in responses}) == 1
    with psycopg.connect(dublin_db) as conn:
        assert conn.execute("SELECT count(*) FROM rental WHERE from_station = 2").fetchone()[0] == 1


def test_helsinki_trip_copies_names_and_replays(helsinki_db, monkeypatch):
    monkeypatch.setenv("LEGACY_B_DB", helsinki_db)
    monkeypatch.setenv("LEGACY_B_USER", "root")
    monkeypatch.setenv("LEGACY_B_PASSWORD", "root")
    from legacy_api import helsinki

    async def scenario():
        async with helsinki.app.router.lifespan_context(helsinki.app), await _client(helsinki) as c:
            key = str(uuid.uuid4())
            body = {
                "departure_station_id": "043",
                "return_station_id": "A04",
                "duration_sec": 600,
                "covered_distance_m": 1800,
            }
            first = await c.post("/matkat", json=body, headers={"Idempotency-Key": key})
            again = await c.post("/matkat", json=body, headers={"Idempotency-Key": key})
            wrong_case = await c.post(
                "/matkat",
                json={**body, "return_station_id": "a04"},
                headers={"Idempotency-Key": str(uuid.uuid4())},
            )
            concurrent = await asyncio.gather(
                *[
                    c.post("/matkat", json=body, headers={"Idempotency-Key": "same-key"})
                    for _ in range(8)
                ]
            )
            return first, again, wrong_case, concurrent

    first, again, wrong_case, concurrent = _run(scenario())
    assert first.status_code == 200 and again.json() == first.json()
    assert wrong_case.status_code == 404  # 'a04' is not 'A04', whatever the collation says
    assert {r.status_code for r in concurrent} == {200}
    assert len({r.json()["id"] for r in concurrent}) == 1
    conn = pymysql.connect(
        host="127.0.0.1", port=3307, user="root", password="root", database=helsinki_db
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), min(departure_station_name), "
            "min(TIMESTAMPDIFF(SECOND, departure, `return`)) FROM matka"
        )
        assert cur.fetchone() == (2, "Karhupuisto", 600)
    conn.close()
