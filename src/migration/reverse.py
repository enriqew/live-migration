"""The reverse replication: after the cutover, the target's changes back into the legacy systems.

python -m migration.reverse

It runs for the rollback window only, so that going back loses nothing written in between. The
reverse connector reads a slot created inside the cutover's fence, after the forward connectors
were paused, so it carries the unified API's writes and nothing else: no change of its own can
come back round through the forward path while it runs, and there is no loop to break.

Each target change becomes the legacy row the spec would have mapped from:

- the legacy key comes from `migration.legacy_key`, which the unified API writes in the same
  transaction as the row (a target id is a hash and cannot be read backwards);
- instants become each system's wall clock again: Dublin's for readings and rentals, Helsinki's,
  to the second, for trips;
- Helsinki trip names are copied from `asema` on insert, as legacy B always did, and left alone on
  correction, as legacy B always did;
- a station change carries only what the unified API can change, name and capacity, so the
  legacy coordinates keep every digit the target rounded away.

Writes are idempotent upserts and deletes by legacy key, applied in topic order, a batch per
legacy database in one transaction. The consumer offsets are committed in the target after the
legacy commit: a crash in between replays the batch, which lands on the same rows.
"""

from __future__ import annotations

import argparse
import signal
import time
from collections import defaultdict
from zoneinfo import ZoneInfo

import psycopg
import pymysql
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from migration.decode import Registry, event

TARGET_DSN = "host=localhost port=5434 dbname=fleet user=fleet password=fleet"
LEGACY_A_DSN = "host=localhost port=5433 dbname=dublin user=dublin password=dublin"
KAFKA = "localhost:29092"
REGISTRY = "http://localhost:8081/apis/registry/v3"
TOPICS = ["fleet.fleet.station", "fleet.fleet.station_status", "fleet.fleet.trip"]
PK = {
    "fleet.fleet.station": "station_id",
    "fleet.fleet.station_status": "status_id",
    "fleet.fleet.trip": "trip_id",
}
DUBLIN = ZoneInfo("Europe/Dublin")
HELSINKI = ZoneInfo("Europe/Helsinki")


def wall(instant, zone: ZoneInfo, seconds: bool = False):
    if instant is None:
        return None
    local = instant.astimezone(zone).replace(tzinfo=None)
    return local.replace(microsecond=0) if seconds else local


class Reverse:
    def __init__(self, batch: int):
        self.batch = batch
        self.registry = Registry(REGISTRY)
        self.stop = False
        self.totals: dict[str, int] = defaultdict(int)

    def connect(self):
        self.target = psycopg.connect(TARGET_DSN, autocommit=True)
        self.dublin = psycopg.connect(LEGACY_A_DSN)
        self.helsinki = pymysql.connect(
            host="127.0.0.1",
            port=3307,
            user="helsinki",
            password="helsinki",
            database="helsinki",
            autocommit=False,
            charset="utf8mb4",
        )

    def keys(self, row_ids: list) -> dict:
        rows = self.target.execute(
            "SELECT row_id, system, legacy_table, legacy_key FROM migration.legacy_key "
            "WHERE row_id = ANY(%s)",
            (row_ids,),
        ).fetchall()
        return {r[0]: (r[1], r[2], r[3]) for r in rows}

    def station_refs(self, station_ids: list) -> dict:
        rows = self.target.execute(
            "SELECT station_id, source_ref FROM fleet.station WHERE station_id = ANY(%s)",
            (station_ids,),
        ).fetchall()
        return dict(rows)

    def apply(self, events: list) -> None:
        ids = [(e.after or e.before)[PK[e.topic]] for e in events]
        keys = self.keys(ids)
        missing = [i for i in ids if i not in keys]
        if missing:
            raise RuntimeError(f"{len(missing)} target rows with no legacy key, e.g. {missing[0]}")
        station_ids = {
            e.after[c]
            for e in events
            if e.after is not None
            for c in ("station_id", "departure_station_id", "return_station_id")
            if c in e.after and e.after[c] is not None
        }
        refs = self.station_refs(list(station_ids)) if station_ids else {}
        a, b = self.dublin.cursor(), self.helsinki.cursor()
        for e in events:
            system, table, key = keys[(e.after or e.before)[PK[e.topic]]]
            row = e.after
            if e.op == "d":
                if table == "station_reading":
                    a.execute("DELETE FROM station_reading WHERE reading_id = %s", (int(key),))
                elif table == "rental":
                    a.execute("DELETE FROM rental WHERE rental_id = %s", (int(key),))
                elif table == "matka":
                    b.execute("DELETE FROM matka WHERE id = %s", (int(key),))
                self.totals[f"{table}.delete"] += 1
                continue
            if table == "station" and system == "dublin":
                a.execute(
                    "UPDATE station SET name = %s, bike_stands = %s WHERE number = %s",
                    (row["name"], row["capacity"], int(key)),
                )
            elif table == "asema":
                b.execute(
                    "UPDATE asema SET nimi = %s, kapasiteetti = %s WHERE id = %s",
                    (row["name"], row["capacity"], key),
                )
            elif table == "station_reading":
                a.execute(
                    "INSERT INTO station_reading (reading_id, station_number, reported_at, "
                    "polled_at, bikes, free_stands, status, is_renting, is_returning) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (reading_id) DO UPDATE SET bikes = EXCLUDED.bikes, "
                    "free_stands = EXCLUDED.free_stands, "
                    "status = EXCLUDED.status, is_renting = EXCLUDED.is_renting, "
                    "is_returning = EXCLUDED.is_returning",
                    (
                        int(key),
                        int(refs[row["station_id"]]),
                        wall(row["reported_at"], DUBLIN),
                        wall(row["polled_at"], DUBLIN),
                        row["bikes_available"],
                        row["docks_available"],
                        "OPEN" if row["is_open"] else "CLOSED",
                        row["is_renting"],
                        row["is_returning"],
                    ),
                )
            elif table == "rental":
                to = row["return_station_id"]
                a.execute(
                    "INSERT INTO rental (rental_id, from_station, to_station, started, ended) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (rental_id) DO UPDATE "
                    "SET to_station = EXCLUDED.to_station, ended = EXCLUDED.ended",
                    (
                        int(key),
                        int(refs[row["departure_station_id"]]),
                        int(refs[to]) if to is not None else None,
                        wall(row["departed_at"], DUBLIN),
                        wall(row["returned_at"], DUBLIN),
                    ),
                )
            elif table == "matka":
                dep = refs.get(row["departure_station_id"])
                ret = refs.get(row["return_station_id"])
                b.execute(
                    "INSERT INTO matka (id, departure, `return`, departure_station_id, "
                    "departure_station_name, return_station_id, return_station_name, "
                    "covered_distance_m, duration_sec) VALUES (%s, %s, %s, %s, "
                    "(SELECT nimi FROM asema WHERE id = %s), %s, "
                    "(SELECT nimi FROM asema x WHERE x.id = %s), %s, %s) "
                    "ON DUPLICATE KEY UPDATE covered_distance_m = VALUES(covered_distance_m), "
                    "duration_sec = VALUES(duration_sec)",
                    (
                        int(key),
                        wall(row["departed_at"], HELSINKI, True),
                        wall(row["returned_at"], HELSINKI, True),
                        dep,
                        dep,
                        ret,
                        ret,
                        row["distance_m"],
                        row["duration_s"],
                    ),
                )
            self.totals[f"{table}.upsert"] += 1
        self.dublin.commit()
        self.helsinki.commit()

    def run(self) -> None:
        self.connect()
        stored = {
            (t, p): o
            for t, p, o in self.target.execute(
                "SELECT topic, partition, next_offset FROM migration.reverse_offset"
            ).fetchall()
        }
        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA,
                "group.id": "reverse",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        assigned: set[tuple[str, int]] = set()

        def refresh() -> None:
            # Debezium creates a topic with its first event: a table the unified API has not
            # written yet has none, so topics are picked up as they appear.
            meta = consumer.list_topics(timeout=10).topics
            new = []
            for topic in TOPICS:
                if topic in meta and meta[topic].error is None:
                    for p in meta[topic].partitions:
                        if (topic, p) not in assigned:
                            assigned.add((topic, p))
                            new.append(
                                TopicPartition(topic, p, stored.get((topic, p), OFFSET_BEGINNING))
                            )
            if new:
                consumer.incremental_assign(new)
                print(f"reverse: now reading {sorted(assigned)}", flush=True)

        signal.signal(signal.SIGINT, lambda *_: setattr(self, "stop", True))
        last_report = last_refresh = time.monotonic()
        refresh()
        try:
            while not self.stop:
                if time.monotonic() - last_refresh > 2:
                    refresh()
                    last_refresh = time.monotonic()
                msgs = consumer.consume(num_messages=self.batch, timeout=0.5)
                events, offsets = [], {}
                for m in msgs:
                    if m.error():
                        raise RuntimeError(m.error())
                    offsets[(m.topic(), m.partition())] = m.offset() + 1
                    if m.value() is None:
                        continue
                    record, semantic = self.registry.decode(m.value())
                    events.append(event(m.topic(), m.partition(), m.offset(), record, semantic))
                if events:
                    self.apply(events)
                if offsets:
                    self.target.cursor().executemany(
                        "INSERT INTO migration.reverse_offset VALUES (%s, %s, %s) "
                        "ON CONFLICT (topic, partition) DO UPDATE "
                        "SET next_offset = EXCLUDED.next_offset",
                        [(t, p, o) for (t, p), o in offsets.items()],
                    )
                if time.monotonic() - last_report > 10:
                    print(f"{time.strftime('%H:%M:%S')} reverse {dict(self.totals)}", flush=True)
                    last_report = time.monotonic()
        finally:
            consumer.close()


def main() -> None:
    p = argparse.ArgumentParser(prog="migration.reverse")
    p.add_argument("--batch", type=int, default=2000)
    Reverse(p.parse_args().batch).run()


if __name__ == "__main__":
    main()
