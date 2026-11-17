"""Legacy B's application: Helsinki trips and stations, over MySQL.

HSL's system records a trip once it is over, so a trip is one insert carrying both ends, with the
station names copied in from `asema` the way the published files carry them. Times are Helsinki
wall clock with no zone. Corrections and erasures are updates and deletes on `matka`; station
changes are updates on `asema`, and they do not touch the names already copied into trips, which
is what the legacy design does and what the migration has to live with.
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiomysql
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from legacy_api.common import Conflict, replayed, request_hash, require_key

HELSINKI = ZoneInfo("Europe/Helsinki")


def wall_clock() -> datetime:
    return datetime.now(HELSINKI).replace(tzinfo=None, microsecond=0)


class TripDone(BaseModel):
    departure_station_id: str
    return_station_id: str
    duration_sec: int = Field(ge=0)
    covered_distance_m: int | None = None


class TripCorrection(BaseModel):
    duration_sec: int | None = Field(None, ge=0)
    covered_distance_m: int | None = None


class StationChange(BaseModel):
    nimi: str | None = None
    kapasiteetti: int | None = Field(None, ge=0)


pool: aiomysql.Pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await aiomysql.create_pool(
        host=os.environ.get("LEGACY_B_HOST", "127.0.0.1"),
        port=int(os.environ.get("LEGACY_B_PORT", "3307")),
        user=os.environ.get("LEGACY_B_USER", "helsinki"),
        password=os.environ.get("LEGACY_B_PASSWORD", "helsinki"),
        db=os.environ.get("LEGACY_B_DB", "helsinki"),
        charset="utf8mb4",
        autocommit=False,
        minsize=2,
        maxsize=20,
    )
    yield
    pool.close()
    await pool.wait_closed()


app = FastAPI(title="Helsinki legacy API", lifespan=lifespan)


async def idempotent(key: str | None, method: str, path: str, body: dict | None, work):
    key = require_key(key)
    h = request_hash(method, path, body)
    async with pool.acquire() as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        try:
            try:
                await cur.execute(
                    "INSERT INTO api_idempotency (`key`, request_hash) VALUES (%s, %s)", (key, h)
                )
            except aiomysql.IntegrityError:
                # The first request with this key has committed (InnoDB held this insert until
                # it did). Its response is the answer.
                await conn.rollback()
                await cur.execute(
                    "SELECT request_hash, response FROM api_idempotency WHERE `key` = %s", (key,)
                )
                row = await cur.fetchone()
                await conn.rollback()
                return replayed(row["request_hash"], row["response"], h)
            try:
                response = await work(cur)
            except Conflict as e:
                raise HTTPException(409, str(e)) from e
            await cur.execute(
                "UPDATE api_idempotency SET response = %s WHERE `key` = %s",
                (json.dumps(response), key),
            )
            await conn.commit()
            return response
        except BaseException:
            await conn.rollback()
            raise


async def _station(cur, station_id: str) -> dict:
    await cur.execute("SELECT id, nimi FROM asema WHERE id = %s", (station_id,))
    row = await cur.fetchone()
    # The column's collation is case and accent insensitive: 'a04' would find 'A04'. Ids are
    # compared again as exact strings, never through MySQL's notion of equality.
    if row is None or row["id"] != station_id:
        raise HTTPException(404, f"no station {station_id!r}")
    return row


@app.post("/matkat")
async def trip_done(body: TripDone, idempotency_key: str | None = Header(None)):
    async def work(cur):
        dep = await _station(cur, body.departure_station_id)
        ret = await _station(cur, body.return_station_id)
        returned = wall_clock()
        departed = returned - timedelta(seconds=body.duration_sec)
        await cur.execute(
            "INSERT INTO matka (departure, `return`, departure_station_id, departure_station_name, "
            "return_station_id, return_station_name, covered_distance_m, duration_sec) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                departed,
                returned,
                dep["id"],
                dep["nimi"],
                ret["id"],
                ret["nimi"],
                body.covered_distance_m,
                body.duration_sec,
            ),
        )
        return {"id": cur.lastrowid}

    return await idempotent(idempotency_key, "POST", "/matkat", body.model_dump(), work)


@app.patch("/matkat/{trip_id}")
async def correct_trip(
    trip_id: int, body: TripCorrection, idempotency_key: str | None = Header(None)
):
    async def work(cur):
        changes = body.model_dump(exclude_none=True)
        if not changes:
            raise HTTPException(422, "nothing to correct")
        sets = ", ".join(f"{column} = %s" for column in changes)
        await cur.execute(f"UPDATE matka SET {sets} WHERE id = %s", (*changes.values(), trip_id))
        await cur.execute("SELECT id FROM matka WHERE id = %s", (trip_id,))
        if await cur.fetchone() is None:
            raise HTTPException(404, f"no trip {trip_id}")
        return {"id": trip_id, **changes}

    body_dict = body.model_dump(exclude_none=True)
    return await idempotent(idempotency_key, "PATCH", f"/matkat/{trip_id}", body_dict, work)


@app.delete("/matkat/{trip_id}")
async def erase_trip(trip_id: int, idempotency_key: str | None = Header(None)):
    async def work(cur):
        await cur.execute("DELETE FROM matka WHERE id = %s", (trip_id,))
        if cur.rowcount == 0:
            raise HTTPException(404, f"no trip {trip_id}")
        return {"id": trip_id, "deleted": True}

    return await idempotent(idempotency_key, "DELETE", f"/matkat/{trip_id}", None, work)


@app.patch("/asemat/{station_id}")
async def change_station(
    station_id: str, body: StationChange, idempotency_key: str | None = Header(None)
):
    async def work(cur):
        changes = body.model_dump(exclude_none=True)
        if not changes:
            raise HTTPException(422, "nothing to change")
        await _station(cur, station_id)
        sets = ", ".join(f"{column} = %s" for column in changes)
        await cur.execute(f"UPDATE asema SET {sets} WHERE id = %s", (*changes.values(), station_id))
        return {"id": station_id, **changes}

    body_dict = body.model_dump(exclude_none=True)
    return await idempotent(idempotency_key, "PATCH", f"/asemat/{station_id}", body_dict, work)


@app.get("/asemat")
async def stations():
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT id FROM asema ORDER BY id")
        return {"asemat": [r[0] for r in await cur.fetchall()]}


@app.get("/matkat/max-id")
async def max_trip_id():
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT COALESCE(MAX(id), 0) FROM matka")
        return {"max_id": (await cur.fetchone())[0]}


@app.get("/health")
async def health():
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT 1")
    return {"ok": True}
