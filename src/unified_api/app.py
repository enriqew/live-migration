"""The unified API: both systems' operations, on the target, after the cutover.

The same paths as the two legacy APIs (`/dublin/...` and `/helsinki/...` through the gateway), the
same idempotency protocol, the same business rules. What changes is where rows go and how they
are built: each write assembles the row the legacy system would have stored and passes it through
the spec's transform (`migration.transform`). The target therefore holds exactly what the spec
would derive had the write landed on the legacy system, keys included, which is what keeps the
verifier's comparison and the reverse replication of a rollback meaningful after the switch.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, FastAPI, Header, HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from legacy_api.common import Conflict, replayed, request_hash, require_key
from legacy_api.dublin import RentalReturn, RentalStart, StationChange, StationService
from legacy_api.helsinki import StationChange as AsemaChange
from legacy_api.helsinki import TripCorrection, TripDone
from migration import transform as tf

DUBLIN = ZoneInfo("Europe/Dublin")
HELSINKI = ZoneInfo("Europe/Helsinki")
DEFAULT_DSN = "host=localhost port=5434 dbname=fleet user=fleet password=fleet"
pool: AsyncConnectionPool


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = AsyncConnectionPool(
        os.environ.get("TARGET_DSN", DEFAULT_DSN), min_size=2, max_size=20, open=False
    )
    await pool.open()
    yield
    await pool.close()


app = FastAPI(title="Unified API", lifespan=lifespan)
dublin = APIRouter(prefix="/dublin")
helsinki = APIRouter(prefix="/helsinki")


def dublin_wall() -> datetime:
    return datetime.now(DUBLIN).replace(tzinfo=None)


def helsinki_wall() -> datetime:
    return datetime.now(HELSINKI).replace(tzinfo=None, microsecond=0)


async def idempotent(key, method, path, body, work):
    key = require_key(key)
    h = request_hash(method, path, body)
    async with pool.connection() as conn, conn.transaction():
        cur = conn.cursor(row_factory=dict_row)
        await cur.execute(
            "INSERT INTO migration.api_idempotency (key, request_hash) VALUES (%s, %s) "
            "ON CONFLICT (key) DO NOTHING RETURNING key",
            (key, h),
        )
        if await cur.fetchone() is None:
            await cur.execute(
                "SELECT request_hash, response FROM migration.api_idempotency WHERE key = %s",
                (key,),
            )
            row = await cur.fetchone()
            return replayed(row["request_hash"], row["response"], h)
        try:
            response = await work(cur)
        except Conflict as e:
            raise HTTPException(409, str(e)) from e
        await cur.execute(
            "UPDATE migration.api_idempotency SET response = %s WHERE key = %s",
            (Jsonb(response), key),
        )
        return response


async def _key(cur, target_table: str, system: str, legacy_table: str, legacy_key) -> None:
    """Record which legacy row a target row is, for the reverse replication of a rollback."""
    await cur.execute(
        "INSERT INTO migration.legacy_key VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
        (
            tf.target_id(system, legacy_table, legacy_key),
            target_table,
            system,
            legacy_table,
            str(legacy_key),
        ),
    )


async def _insert(cur, table: str, row: dict) -> None:
    cols = list(row)
    await cur.execute(
        f"INSERT INTO fleet.{table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
        [row[c] for c in cols],
    )


# --- Dublin -------------------------------------------------------------------------------------


async def _latest_status(cur, number: int) -> dict:
    sid = tf.target_id("dublin", "station", number)
    await cur.execute("SELECT pg_advisory_xact_lock(%s)", (number,))
    await cur.execute(
        "SELECT s.station_id, st.bikes_available, st.docks_available, st.is_open, "
        "st.is_renting, st.is_returning FROM fleet.station s "
        "LEFT JOIN LATERAL (SELECT * FROM fleet.station_status WHERE station_id = s.station_id "
        "  ORDER BY reported_at DESC, status_id LIMIT 1) st ON true "
        "WHERE s.station_id = %s",
        (sid,),
    )
    row = await cur.fetchone()
    if row is None:
        raise HTTPException(404, f"no station {number}")
    if row["bikes_available"] is None:
        raise Conflict(f"station {number} has never reported")
    return row


async def _reading(cur, number: int, last: dict, bikes: int, free: int, now: datetime, **change):
    await cur.execute("SELECT nextval('migration.seq_dublin_reading')")
    reading_id = (await cur.fetchone())["nextval"]
    state = {
        "is_open": last["is_open"],
        "is_renting": last["is_renting"],
        "is_returning": last["is_returning"],
    }
    if "status" in change:
        state["is_open"] = change.pop("status") == "OPEN"
    state.update(change)
    legacy = {
        "reading_id": reading_id,
        "station_number": number,
        "reported_at": now,
        "polled_at": None,
        "bikes": bikes,
        "free_stands": free,
        "status": "OPEN" if state["is_open"] else "CLOSED",
        "is_renting": state["is_renting"],
        "is_returning": state["is_returning"],
    }
    row = tf.dublin_reading(legacy, {})
    if isinstance(row, tf.Quarantine):
        raise HTTPException(503, f"reading refused by the spec: {row.rule}")
    await _key(cur, "station_status", "dublin", "station_reading", reading_id)
    await _insert(cur, "station_status", row)
    return reading_id


@dublin.get("/stations")
async def stations():
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT s.source_ref::int, coalesce(st.is_open AND st.is_renting IS NOT FALSE "
            "  AND st.is_returning IS NOT FALSE, false) "
            "FROM fleet.station s LEFT JOIN LATERAL (SELECT * FROM fleet.station_status "
            "  WHERE station_id = s.station_id ORDER BY reported_at DESC, status_id LIMIT 1) st "
            "  ON true WHERE s.system_code = 'dublin' ORDER BY 1"
        )
        rows = await cur.fetchall()
    return {"stations": [n for n, _ in rows], "out_of_service": [n for n, ok in rows if not ok]}


@dublin.post("/rentals")
async def start_rental(body: RentalStart, idempotency_key: str | None = Header(None)):
    async def work(cur):
        last = await _latest_status(cur, body.from_station)
        if last["bikes_available"] <= 0 or not last["is_open"] or last["is_renting"] is False:
            raise Conflict(f"no bike to rent at station {body.from_station}")
        now = dublin_wall()
        await cur.execute("SELECT nextval('migration.seq_dublin_rental')")
        rental_id = (await cur.fetchone())["nextval"]
        trip = tf.dublin_rental(
            {
                "rental_id": rental_id,
                "from_station": body.from_station,
                "to_station": None,
                "started": now,
                "ended": None,
            }
        )
        if isinstance(trip, tf.Quarantine):
            raise HTTPException(503, f"rental refused by the spec: {trip.rule}")
        await _key(cur, "trip", "dublin", "rental", rental_id)
        await _insert(cur, "trip", trip)
        await _reading(
            cur,
            body.from_station,
            last,
            last["bikes_available"] - 1,
            last["docks_available"] + 1,
            now,
        )
        return {"rental_id": rental_id}

    return await idempotent(idempotency_key, "POST", "/rentals", body.model_dump(), work)


@dublin.post("/rentals/{rental_id}/return")
async def return_rental(
    rental_id: int, body: RentalReturn, idempotency_key: str | None = Header(None)
):
    async def work(cur):
        tid = tf.target_id("dublin", "rental", rental_id)
        await cur.execute(
            "SELECT departure_station_id, departed_at, return_station_id "
            "FROM fleet.trip WHERE trip_id = %s FOR UPDATE",
            (tid,),
        )
        row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, f"no rental {rental_id}")
        if row["return_station_id"] is not None:
            raise Conflict(f"rental {rental_id} is already returned")
        last = await _latest_status(cur, body.to_station)
        if last["docks_available"] <= 0 or not last["is_open"] or last["is_returning"] is False:
            raise Conflict(f"no free dock at station {body.to_station}")
        now = dublin_wall()
        await cur.execute(
            "SELECT source_ref::int AS n FROM fleet.station WHERE station_id = %s",
            (row["departure_station_id"],),
        )
        from_station = (await cur.fetchone())["n"]
        started = row["departed_at"].astimezone(DUBLIN).replace(tzinfo=None)
        trip = tf.dublin_rental(
            {
                "rental_id": rental_id,
                "from_station": from_station,
                "to_station": body.to_station,
                "started": started,
                "ended": now,
            }
        )
        if isinstance(trip, tf.Quarantine):
            raise HTTPException(503, f"rental refused by the spec: {trip.rule}")
        await _key(cur, "trip", "dublin", "rental", rental_id)
        await cur.execute(
            "UPDATE fleet.trip SET return_station_id = %s, returned_at = %s, duration_s = %s, "
            "flags = %s WHERE trip_id = %s",
            (
                trip["return_station_id"],
                trip["returned_at"],
                trip["duration_s"],
                trip["flags"],
                tid,
            ),
        )
        await _reading(
            cur,
            body.to_station,
            last,
            last["bikes_available"] + 1,
            last["docks_available"] - 1,
            now,
        )
        return {"rental_id": rental_id}

    path = f"/rentals/{rental_id}/return"
    return await idempotent(idempotency_key, "POST", path, body.model_dump(), work)


@dublin.delete("/rentals/{rental_id}")
async def erase_rental(rental_id: int, idempotency_key: str | None = Header(None)):
    async def work(cur):
        await _key(cur, "trip", "dublin", "rental", rental_id)
        await cur.execute(
            "DELETE FROM fleet.trip WHERE trip_id = %s RETURNING trip_id",
            (tf.target_id("dublin", "rental", rental_id),),
        )
        if await cur.fetchone() is None:
            raise HTTPException(404, f"no rental {rental_id}")
        return {"rental_id": rental_id, "deleted": True}

    return await idempotent(idempotency_key, "DELETE", f"/rentals/{rental_id}", None, work)


@dublin.patch("/stations/{number}")
async def change_station(
    number: int, body: StationChange, idempotency_key: str | None = Header(None)
):
    async def work(cur):
        await _key(cur, "station", "dublin", "station", number)
        await cur.execute(
            "UPDATE fleet.station SET name = COALESCE(%s, name), capacity = COALESCE(%s, capacity) "
            "WHERE station_id = %s RETURNING name, capacity",
            (tf.clean_name(body.name), body.bike_stands, tf.target_id("dublin", "station", number)),
        )
        row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, f"no station {number}")
        return {"number": number, "name": row["name"], "bike_stands": row["capacity"]}

    body_dict = body.model_dump(exclude_none=True)
    return await idempotent(idempotency_key, "PATCH", f"/stations/{number}", body_dict, work)


@dublin.post("/stations/{number}/service")
async def station_service(
    number: int, body: StationService, idempotency_key: str | None = Header(None)
):
    async def work(cur):
        last = await _latest_status(cur, number)
        change = body.model_dump(exclude_none=True)
        if change.get("status") not in (None, "OPEN", "CLOSED"):
            raise HTTPException(422, "status is OPEN or CLOSED")
        reading_id = await _reading(
            cur,
            number,
            last,
            last["bikes_available"],
            last["docks_available"],
            dublin_wall(),
            **change,
        )
        return {"reading_id": reading_id}

    body_dict = body.model_dump(exclude_none=True)
    return await idempotent(idempotency_key, "POST", f"/stations/{number}/service", body_dict, work)


# --- Helsinki -----------------------------------------------------------------------------------


async def _asema(cur, station_id: str) -> None:
    await cur.execute(
        "SELECT 1 FROM fleet.station WHERE station_id = %s AND kind <> 'unlisted'",
        (tf.target_id("helsinki", "asema", station_id),),
    )
    if await cur.fetchone() is None:
        raise HTTPException(404, f"no station {station_id!r}")


@helsinki.get("/asemat")
async def asemat():
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT source_ref FROM fleet.station WHERE system_code = "
            "'helsinki' AND kind <> 'unlisted' ORDER BY 1"
        )
        return {"asemat": [r[0] for r in await cur.fetchall()]}


@helsinki.get("/matkat/max-id")
async def max_trip_id():
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT last_value FROM migration.seq_helsinki_matka")
        return {"max_id": (await cur.fetchone())[0]}


@helsinki.post("/matkat")
async def trip_done(body: TripDone, idempotency_key: str | None = Header(None)):
    async def work(cur):
        await _asema(cur, body.departure_station_id)
        await _asema(cur, body.return_station_id)
        returned = helsinki_wall()
        await cur.execute("SELECT nextval('migration.seq_helsinki_matka')")
        trip_id = (await cur.fetchone())["nextval"]
        distance = None if body.covered_distance_m is None else str(body.covered_distance_m)
        trip = tf.helsinki_trip(
            {
                "id": trip_id,
                "departure": returned - timedelta(seconds=body.duration_sec),
                "return": returned,
                "departure_station_id": body.departure_station_id,
                "return_station_id": body.return_station_id,
                "covered_distance_m": distance,
                "duration_sec": body.duration_sec,
            }
        )
        if isinstance(trip, tf.Quarantine):
            raise HTTPException(503, f"trip refused by the spec: {trip.rule}")
        await _key(cur, "trip", "helsinki", "matka", trip_id)
        await _insert(cur, "trip", trip)
        return {"id": trip_id}

    return await idempotent(idempotency_key, "POST", "/matkat", body.model_dump(), work)


@helsinki.patch("/matkat/{trip_id}")
async def correct_trip(
    trip_id: int, body: TripCorrection, idempotency_key: str | None = Header(None)
):
    async def work(cur):
        changes = body.model_dump(exclude_none=True)
        if not changes:
            raise HTTPException(422, "nothing to correct")
        tid = tf.target_id("helsinki", "matka", trip_id)
        await cur.execute("SELECT flags FROM fleet.trip WHERE trip_id = %s FOR UPDATE", (tid,))
        row = await cur.fetchone()
        if row is None:
            raise HTTPException(404, f"no trip {trip_id}")
        flags = set(row["flags"])
        sets, values = [], []
        if "duration_sec" in changes:
            sets.append("duration_s = %s")
            values.append(changes["duration_sec"])
        if "covered_distance_m" in changes:
            d = changes["covered_distance_m"]
            flags.discard("negative_distance")
            if d < 0:
                flags.add("negative_distance")
            sets.append("distance_m = %s")
            values.append(None if d < 0 else d)
        sets.append("flags = %s")
        values.append(sorted(flags))
        await _key(cur, "trip", "helsinki", "matka", trip_id)
        await cur.execute(
            f"UPDATE fleet.trip SET {', '.join(sets)} WHERE trip_id = %s", (*values, tid)
        )
        return {"id": trip_id, **changes}

    body_dict = body.model_dump(exclude_none=True)
    return await idempotent(idempotency_key, "PATCH", f"/matkat/{trip_id}", body_dict, work)


@helsinki.delete("/matkat/{trip_id}")
async def erase_trip(trip_id: int, idempotency_key: str | None = Header(None)):
    async def work(cur):
        await _key(cur, "trip", "helsinki", "matka", trip_id)
        await cur.execute(
            "DELETE FROM fleet.trip WHERE trip_id = %s RETURNING trip_id",
            (tf.target_id("helsinki", "matka", trip_id),),
        )
        if await cur.fetchone() is None:
            raise HTTPException(404, f"no trip {trip_id}")
        return {"id": trip_id, "deleted": True}

    return await idempotent(idempotency_key, "DELETE", f"/matkat/{trip_id}", None, work)


@helsinki.patch("/asemat/{station_id}")
async def change_asema(
    station_id: str, body: AsemaChange, idempotency_key: str | None = Header(None)
):
    async def work(cur):
        changes = body.model_dump(exclude_none=True)
        if not changes:
            raise HTTPException(422, "nothing to change")
        await _asema(cur, station_id)
        await _key(cur, "station", "helsinki", "asema", station_id)
        await cur.execute(
            "UPDATE fleet.station SET name = COALESCE(%s, name), capacity = COALESCE(%s, capacity) "
            "WHERE station_id = %s",
            (
                tf.clean_name(changes.get("nimi")),
                changes.get("kapasiteetti"),
                tf.target_id("helsinki", "asema", station_id),
            ),
        )
        return {"id": station_id, **changes}

    body_dict = body.model_dump(exclude_none=True)
    return await idempotent(idempotency_key, "PATCH", f"/asemat/{station_id}", body_dict, work)


@app.get("/{system}/health")
async def health(system: str):
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")
    return {"ok": True}


app.include_router(dublin)
app.include_router(helsinki)
