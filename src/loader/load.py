"""Bulk load of the locked files into the two legacy systems.

This is the initial state of each legacy system, before any migration exists: COPY into
PostgreSQL, LOAD DATA into MySQL, one transaction per file. It refuses to load into a system that
already has rows unless told to reset it, so a half-finished load is never extended by accident.

`LOAD DATA LOCAL` turns errors into warnings and keeps going: a value too long is truncated, a bad
number becomes 0. So every file is checked for an exact row count and zero warnings, and rolled
back otherwise. A silent coercion at load would be a defect nobody put in the data.
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pymysql

from loader import dublin, helsinki
from loader.catalog import SourceFile
from loader.fetch import local_path


@dataclass(frozen=True)
class Endpoints:
    legacy_a: str = "host=localhost port=5433 dbname=dublin user=dublin password=dublin"
    legacy_b_host: str = "127.0.0.1"
    legacy_b_port: int = 3307


class NotEmpty(RuntimeError):
    pass


def tsv_field(value: object) -> str:
    """A field in the text format COPY and LOAD DATA both read: \\N for null, backslash escapes."""
    if value is None:
        return "\\N"
    if isinstance(value, bool):
        return "t" if value else "f"
    text = str(value)
    return text.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def tsv_line(values: Iterable[object]) -> str:
    return "\t".join(tsv_field(v) for v in values) + "\n"


# --- legacy A: Dublin, PostgreSQL ---------------------------------------------------------------

_STATION_UPSERT = """
    INSERT INTO station (number, name, address, latitude, longitude, bike_stands)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (number) DO UPDATE SET
        name = EXCLUDED.name, address = EXCLUDED.address, latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude, bike_stands = EXCLUDED.bike_stands
"""
_READING_COLUMNS = (
    "station_number, reported_at, polled_at, bikes, free_stands, status, is_renting, is_returning"
)


def load_dublin(files: list[SourceFile], raw_dir: Path, ep: Endpoints, reset: bool) -> int:
    total = 0
    with psycopg.connect(ep.legacy_a) as conn:
        _guard_empty_pg(conn, reset)
        # Files in time order: the station attributes of the latest file win, and reading_id
        # follows real time across files as well as within them.
        for f in sorted(files, key=lambda f: f.months[0]):
            stations, readings, dropped = dublin.parse(local_path(raw_dir, f), f.era, set(f.months))
            with conn.transaction(), conn.cursor() as cur:
                cur.executemany(
                    _STATION_UPSERT,
                    [
                        (s.number, s.name, s.address, s.latitude, s.longitude, s.bike_stands)
                        for s in stations.values()
                    ],
                )
                with cur.copy(f"COPY station_reading ({_READING_COLUMNS}) FROM STDIN") as copy:
                    for r in readings:
                        copy.write(
                            tsv_line(
                                (
                                    r.station_number,
                                    r.reported_at,
                                    r.polled_at,
                                    r.bikes,
                                    r.free_stands,
                                    r.status,
                                    r.is_renting,
                                    r.is_returning,
                                )
                            )
                        )
            total += len(readings)
            note = (
                f", {dropped:,} rows outside {f.months[0]}..{f.months[-1]} dropped"
                if dropped
                else ""
            )
            print(f"legacy A: {f.name}: {len(readings):,} readings, {len(stations)} stations{note}")
    return total


def _guard_empty_pg(conn: psycopg.Connection, reset: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM station_reading) OR EXISTS (SELECT 1 FROM rental)"
        )
        if cur.fetchone()[0]:
            if not reset:
                raise NotEmpty("legacy A already has rows; pass --reset to empty it first")
            # The API's idempotency record too: a reloaded system remembers no client keys.
            cur.execute(
                "TRUNCATE rental, station_reading, station, api_idempotency RESTART IDENTITY"
            )
    conn.commit()


# --- legacy B: Helsinki, MySQL ------------------------------------------------------------------

_ASEMA_COLUMNS = "id, nimi, namn, name, osoite, adress, kaupunki, operaattori, kapasiteetti, x, y"
_MATKA_COLUMNS = (
    "departure, `return`, departure_station_id, departure_station_name, return_station_id, "
    "return_station_name, covered_distance_m, duration_sec"
)


def _load_data(cur, path: Path, table: str, columns: str) -> None:
    cur.execute(
        f"LOAD DATA LOCAL INFILE %s INTO TABLE {table} CHARACTER SET utf8mb4 "
        f"FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' ({columns})",
        (str(path),),
    )


def load_helsinki(files: list[SourceFile], raw_dir: Path, ep: Endpoints, reset: bool) -> int:
    conn = pymysql.connect(
        host=ep.legacy_b_host,
        port=ep.legacy_b_port,
        user="helsinki",
        password="helsinki",
        database="helsinki",
        charset="utf8mb4",
        local_infile=True,
        autocommit=False,
    )
    total = 0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT (SELECT COUNT(*) FROM matka) + (SELECT COUNT(*) FROM asema)")
            if cur.fetchone()[0]:
                if not reset:
                    raise NotEmpty("legacy B already has rows; pass --reset to empty it first")
                cur.execute("TRUNCATE TABLE matka")
                cur.execute("TRUNCATE TABLE asema")
                cur.execute("TRUNCATE TABLE api_idempotency")
        with tempfile.TemporaryDirectory() as stage:
            for f in sorted(files, key=lambda f: (f.kind != "stations", f.months)):
                staged = Path(stage) / f"{f.name}.tsv"
                path = local_path(raw_dir, f)
                if f.kind == "stations":
                    rows = helsinki.parse_stations(path)
                    with staged.open("w", encoding="utf-8", newline="\n") as out:
                        for s in rows:
                            out.write(
                                tsv_line(
                                    (
                                        s.id,
                                        s.nimi,
                                        s.namn,
                                        s.name,
                                        s.osoite,
                                        s.adress,
                                        s.kaupunki,
                                        s.operaattori,
                                        s.kapasiteetti,
                                        s.x,
                                        s.y,
                                    )
                                )
                            )
                    table, columns = "asema", _ASEMA_COLUMNS
                else:
                    rows = helsinki.parse_trips(path)
                    with staged.open("w", encoding="utf-8", newline="\n") as out:
                        for t in rows:
                            out.write(
                                tsv_line(
                                    (
                                        t.departure,
                                        t.return_,
                                        t.departure_station_id,
                                        t.departure_station_name,
                                        t.return_station_id,
                                        t.return_station_name,
                                        t.covered_distance_m,
                                        t.duration_sec,
                                    )
                                )
                            )
                    table, columns = "matka", _MATKA_COLUMNS
                with conn.cursor() as cur:
                    _load_data(cur, staged, table, columns)
                    loaded = cur.rowcount
                    cur.execute("SHOW COUNT(*) WARNINGS")
                    warnings = cur.fetchone()[0]
                if loaded != len(rows) or warnings:
                    conn.rollback()
                    raise RuntimeError(
                        f"legacy B: {f.name}: {loaded:,} of {len(rows):,} rows loaded, "
                        f"{warnings} warnings; nothing from this file was kept"
                    )
                conn.commit()
                if f.kind == "trips":
                    total += loaded
                print(f"legacy B: {f.name}: {loaded:,} rows into {table}")
    finally:
        conn.close()
    return total
