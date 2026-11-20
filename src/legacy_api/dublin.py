"""Legacy A's application: Dublin rentals and station state, over PostgreSQL.

It behaves the way the legacy system's data says it did: times are Dublin wall clock with no
zone, and station state is a series of readings. Renting or returning a bike writes the rental and
a new reading of the station in one transaction, so the change stream carries transactions that
span two tables, which the migration must apply atomically.

Per station, writes are serialized with a transaction-scoped advisory lock: the new reading is
computed from the latest one, and two rentals at the same station must not both read it.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Header, HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

from legacy_api.common import Conflict, gate, replayed, request_hash, require_key

DUBLIN = ZoneInfo("Europe/Dublin")
DEFAULT_DSN = "host=localhost port=5433 dbname=dublin user=dublin password=dublin"


def wall_clock() -> datetime:
    return datetime.now(DUBLIN).replace(tzinfo=None)


class RentalStart(BaseModel):
    from_station: int


class RentalReturn(BaseModel):
    to_station: int


class StationChange(BaseModel):
    name: str | None = None
    bike_stands: int | None = None


class StationService(BaseModel):
    status: str | None = None  # 'OPEN' | 'CLOSED'
    is_renting: bool | None = None
    is_returning: bool | None = None


pool: AsyncConnectionPool


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = AsyncConnectionPool(
        os.environ.get("LEGACY_A_DSN", DEFAULT_DSN), min_size=2, max_size=20, open=False
    )
    await pool.open()
    yield
    await pool.close()


app = FastAPI(title="Dublin legacy API", lifespan=lifespan)


async def idempotent(key: str | None, method: str, path: str, body: dict | None, work):
    gate.check()
    key = require_key(key)
    h = request_hash(method, path, body)
    async with pool.connection() as conn, conn.transaction():
        cur = conn.cursor(row_factory=dict_row)
        await cur.execute(
            "INSERT INTO api_idempotency (key, request_hash) VALUES (%s, %s) "
            "ON CONFLICT (key) DO NOTHING RETURNING key",
            (key, h),
        )
        if await cur.fetchone() is None:
            await cur.execute(
                "SELECT request_hash, response FROM api_idempotency WHERE key = %s", (key,)
            )
            row = await cur.fetchone()
            return replayed(row["request_hash"], row["response"], h)
        try:
            response = await work(cur)
        except Conflict as e:
            raise HTTPException(409, str(e)) from e
        await cur.execute(
            "UPDATE api_idempotency SET response = %s WHERE key = %s", (Jsonb(response), key)
        )
        return response


async def _lock_station(cur, number: int) -> dict:
    await cur.execute("SELECT pg_advisory_xact_lock(%s)", (number,))
    await cur.execute(
        "SELECT r.bikes, r.free_stands, r.status, r.is_renting, r.is_returning "
        "FROM station s LEFT JOIN LATERAL (SELECT * FROM station_reading "
        "  WHERE station_number = s.number ORDER BY reading_id DESC LIMIT 1) r ON true "
        "WHERE s.number = %s",
        (number,),
    )
    row = await cur.fetchone()
    if row is None:
        raise HTTPException(404, f"no station {number}")
    if row["bikes"] is None:
        raise Conflict(f"station {number} has never reported")
    return row


async def _reading(cur, number: int, last: dict, bikes: int, free: int, now: datetime, **change):
    state = {
        "status": last["status"],
        "is_renting": last["is_renting"],
        "is_returning": last["is_returning"],
        **change,
    }
    await cur.execute(
        "INSERT INTO station_reading (station_number, reported_at, bikes, free_stands, status, "
        "is_renting, is_returning) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING reading_id",
        (number, now, bikes, free, state["status"], state["is_renting"], state["is_returning"]),
    )
    return (await cur.fetchone())["reading_id"]


@app.post("/rentals")
async def start_rental(body: RentalStart, idempotency_key: str | None = Header(None)):
    async def work(cur):
        last = await _lock_station(cur, body.from_station)
        if last["bikes"] <= 0 or last["status"] != "OPEN" or last["is_renting"] is False:
            raise Conflict(f"no bike to rent at station {body.from_station}")
        now = wall_clock()
        await cur.execute(
            "INSERT INTO rental (from_station, started) VALUES (%s, %s) RETURNING rental_id",
            (body.from_station, now),
        )
        rental_id = (await cur.fetchone())["rental_id"]
        await _reading(
            cur, body.from_station, last, last["bikes"] - 1, last["free_stands"] + 1, now
        )
        return {"rental_id": rental_id}

    return await idempotent(idempotency_key, "POST", "/rentals", body.model_dump(), work)


@app.post("/rentals/{rental_id}/return")
async def return_rental(
    rental_id: int, body: RentalReturn, idempotency_key: str | None = Header(None)
):
    async def work(cur):
        await cur.execute(
            "SELECT to_station FROM rental WHERE rental_id = %s FOR UPDATE", (rental_id,)
        )
        row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, f"no rental {rental_id}")
        if row["to_station"] is not None:
            raise Conflict(f"rental {rental_id} is already returned")
        last = await _lock_station(cur, body.to_station)
        if last["free_stands"] <= 0 or last["status"] != "OPEN" or last["is_returning"] is False:
            raise Conflict(f"no free dock at station {body.to_station}")
        now = wall_clock()
        await cur.execute(
            "UPDATE rental SET to_station = %s, ended = %s WHERE rental_id = %s",
            (body.to_station, now, rental_id),
        )
        await _reading(cur, body.to_station, last, last["bikes"] + 1, last["free_stands"] - 1, now)
        return {"rental_id": rental_id}

    path = f"/rentals/{rental_id}/return"
    return await idempotent(idempotency_key, "POST", path, body.model_dump(), work)


@app.delete("/rentals/{rental_id}")
async def erase_rental(rental_id: int, idempotency_key: str | None = Header(None)):
    """An erasure request: the rental is deleted, the station readings it caused stay."""

    async def work(cur):
        await cur.execute(
            "DELETE FROM rental WHERE rental_id = %s RETURNING rental_id", (rental_id,)
        )
        if await cur.fetchone() is None:
            raise HTTPException(404, f"no rental {rental_id}")
        return {"rental_id": rental_id, "deleted": True}

    return await idempotent(idempotency_key, "DELETE", f"/rentals/{rental_id}", None, work)


@app.patch("/stations/{number}")
async def change_station(
    number: int, body: StationChange, idempotency_key: str | None = Header(None)
):
    async def work(cur):
        await cur.execute(
            "UPDATE station SET name = COALESCE(%s, name), bike_stands = COALESCE(%s, bike_stands) "
            "WHERE number = %s RETURNING number, name, bike_stands",
            (body.name, body.bike_stands, number),
        )
        row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, f"no station {number}")
        return dict(row)

    body_dict = body.model_dump(exclude_none=True)
    return await idempotent(idempotency_key, "PATCH", f"/stations/{number}", body_dict, work)


@app.post("/stations/{number}/service")
async def station_service(
    number: int, body: StationService, idempotency_key: str | None = Header(None)
):
    """A station taken out of service or back: a new reading with the same counts."""

    async def work(cur):
        last = await _lock_station(cur, number)
        change = body.model_dump(exclude_none=True)
        if change.get("status") not in (None, "OPEN", "CLOSED"):
            raise HTTPException(422, "status is OPEN or CLOSED")
        reading_id = await _reading(
            cur, number, last, last["bikes"], last["free_stands"], wall_clock(), **change
        )
        return {"reading_id": reading_id}

    body_dict = body.model_dump(exclude_none=True)
    return await idempotent(idempotency_key, "POST", f"/stations/{number}/service", body_dict, work)


@app.get("/stations")
async def stations():
    """Every station, and whether its latest reading has it fully in service."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT s.number, COALESCE(r.status = 'OPEN' AND r.is_renting IS NOT FALSE "
            "  AND r.is_returning IS NOT FALSE, false) "
            "FROM station s LEFT JOIN LATERAL (SELECT status, is_renting, is_returning "
            "  FROM station_reading WHERE station_number = s.number "
            "  ORDER BY reading_id DESC LIMIT 1) r ON true ORDER BY s.number"
        )
        rows = await cur.fetchall()
    return {
        "stations": [n for n, _ in rows],
        "out_of_service": [n for n, ok in rows if not ok],
    }


@app.post("/admin/writes")
async def set_writes(open: bool):
    gate.open = open
    return {"writes_open": gate.open}


@app.get("/health")
async def health():
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")
    return {"ok": True}
