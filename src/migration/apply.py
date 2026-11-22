"""The apply service: one legacy system's change stream into the target.

python -m migration.apply dublin|helsinki [--batch 5000]

At-least-once in, exactly-once in effect:

- **Version guard.** Every change carries its source position. A change is applied only if its
  position is newer than the one `migration.row_version` holds for that target row, deletes
  included (a delete leaves a tombstone). Replays, duplicates and old changes arriving late are
  no-ops, so the result does not depend on the order or the number of deliveries.
- **Source transactions stay whole.** A change that belongs to a source transaction waits until
  the transaction's END has arrived and every one of its events has, counted per table from the
  END record. Then all of them go into the same target transaction. Snapshot reads belong to no
  transaction and are ready at once.
- **Offsets live in the target.** Each batch writes, in its own transaction, the next offset to
  read for every partition: the lowest offset not yet applied. A crash loses nothing and repeats
  only what the version guard turns into no-ops.
- **Fail closed on schema drift.** A column the spec has no rule for stops the service.
- **Checksum markers.** A row in a legacy system's `verify_marker` table, written by the
  continuous verifier in the transaction that reads a chunk. The service applies it only once
  every source transaction that committed before it is in the target, and holds back every one
  that committed after it; then, in the marker's own target transaction, it sets the target's rows
  for that chunk aside. What it sets aside is the target exactly at the marker's source position.
  The service computes nothing about those rows: comparing them is the verifier's job.
"""

from __future__ import annotations

import argparse
import heapq
import json
import os
import signal
import time
from collections import defaultdict
from dataclasses import dataclass, field

import psycopg
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from migration import folds as fold_map
from migration import transform as tf
from migration.decode import Registry, event

TARGET_DSN = "host=localhost port=5434 dbname=fleet user=fleet password=fleet"
LEGACY_A_DSN = "host=localhost port=5433 dbname=dublin user=dublin password=dublin"
REGISTRY = "http://localhost:8081/apis/registry/v3"
KAFKA = "localhost:29092"


@dataclass(frozen=True)
class Table:
    system: str
    legacy_table: str
    target_table: str
    key: str
    columns: frozenset[str]


TABLES = {
    "dublin.public.station": Table(
        "dublin",
        "station",
        "station",
        "number",
        frozenset({"number", "name", "address", "latitude", "longitude", "bike_stands"}),
    ),
    "dublin.public.station_reading": Table(
        "dublin",
        "station_reading",
        "station_status",
        "reading_id",
        frozenset(
            {
                "reading_id",
                "station_number",
                "reported_at",
                "polled_at",
                "bikes",
                "free_stands",
                "status",
                "is_renting",
                "is_returning",
            }
        ),
    ),
    "dublin.public.rental": Table(
        "dublin",
        "rental",
        "trip",
        "rental_id",
        frozenset({"rental_id", "from_station", "to_station", "started", "ended"}),
    ),
    "helsinki.helsinki.asema": Table(
        "helsinki",
        "asema",
        "station",
        "id",
        frozenset(
            {
                "id",
                "nimi",
                "namn",
                "name",
                "osoite",
                "adress",
                "kaupunki",
                "operaattori",
                "kapasiteetti",
                "x",
                "y",
            }
        ),
    ),
    "helsinki.helsinki.matka": Table(
        "helsinki",
        "matka",
        "trip",
        "id",
        frozenset(
            {
                "id",
                "departure",
                "return",
                "departure_station_id",
                "departure_station_name",
                "return_station_id",
                "return_station_name",
                "covered_distance_m",
                "duration_sec",
            }
        ),
    ),
}
KEY_TABLE = {
    "station": "station",
    "station_reading": "station_reading",
    "rental": "rental",
    "asema": "asema",
    "matka": "matka",
}
PK = {"station": "station_id", "station_status": "status_id", "trip": "trip_id"}
COLUMNS = {
    "station": [
        "station_id",
        "system_code",
        "source_ref",
        "name",
        "name_sv",
        "name_en",
        "address",
        "city",
        "capacity",
        "latitude",
        "longitude",
        "kind",
    ],
    "station_status": [
        "status_id",
        "station_id",
        "reported_at",
        "polled_at",
        "bikes_available",
        "docks_available",
        "is_open",
        "is_renting",
        "is_returning",
    ],
    "trip": [
        "trip_id",
        "system_code",
        "departure_station_id",
        "return_station_id",
        "departed_at",
        "returned_at",
        "duration_s",
        "distance_m",
        "flags",
    ],
}


class SchemaDrift(RuntimeError):
    pass


MARKER_TOPICS = {
    "dublin": "dublin.public.verify_marker",
    "helsinki": "helsinki.helsinki.verify_marker",
}

# A marker names a legacy table and a key range. The target rows of that range are found by their
# ids, which PostgreSQL derives here with its own uuid_generate_v5: the copy does not depend on the
# migration's id function, so a wrong id in the target shows as a missing row.
MARKED = {
    "station_reading": ("dublin", "station_status", "dublin:station_reading:"),
    "rental": ("dublin", "trip", "dublin:rental:"),
    "matka": ("helsinki", "trip", "helsinki:matka:"),
    "station": ("dublin", "station", None),
    "asema": ("helsinki", "station", None),
}


@dataclass(frozen=True)
class Marker:
    marker_id: str
    legacy_table: str
    lo: int
    hi: int


def set_aside(cur, system: str, m: Marker) -> bool:
    """Record the marker and copy the target's rows for its chunk aside. False if seen before."""
    cur.execute(
        "INSERT INTO migration.verify_marker_applied (marker_id, source, legacy_table, lo, hi) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING marker_id",
        (m.marker_id, system, m.legacy_table, m.lo, m.hi),
    )
    if cur.fetchone() is None or m.legacy_table not in MARKED:
        return False
    source, table, prefix = MARKED[m.legacy_table]
    if prefix is None:
        # The whole station register. Helsinki's stubs depend on every trip, not on the register,
        # and are left to the frozen check.
        cur.execute(
            "INSERT INTO migration.snap_station SELECT %s, s.* FROM fleet.station s "
            "WHERE s.system_code = %s AND s.kind <> 'unlisted'",
            (m.marker_id, source),
        )
        cur.execute(
            "INSERT INTO migration.snap_quarantine SELECT %s, q.* FROM migration.quarantine q "
            "WHERE q.source = %s AND q.legacy_table = %s",
            (m.marker_id, source, m.legacy_table),
        )
        return True
    cur.execute(
        f"INSERT INTO migration.snap_{table} SELECT %s, x.* FROM fleet.{table} x "
        f"WHERE x.{PK[table]} IN (SELECT uuid_generate_v5(%s::uuid, %s || k) "
        "FROM generate_series(%s::bigint, %s::bigint) AS k)",
        (m.marker_id, str(tf.NAMESPACE), prefix, m.lo, m.hi),
    )
    cur.execute(
        "INSERT INTO migration.snap_quarantine SELECT %s, q.* FROM migration.quarantine q "
        "WHERE q.source = %s AND q.legacy_table = %s AND q.legacy_key IN "
        "(SELECT k::text FROM generate_series(%s::bigint, %s::bigint) AS k)",
        (m.marker_id, source, m.legacy_table, m.lo, m.hi),
    )
    return True


@dataclass
class Change:
    target_table: str
    row_id: object
    pos: tuple[int, int]
    kind: str  # 'upsert' | 'delete' | 'quarantine'
    row: dict | None
    legacy: tuple[str, str, str]  # source, legacy table, legacy key
    quarantine: tuple[str, str, dict] | None = None  # rule, detail, raw row
    old_stations: tuple = ()  # Helsinki station ids a trip named before this change
    new_stations: tuple = ()  # and the ones it names now, which need a station or a stub


def position(system: str, source: dict) -> tuple[int, int]:
    """Debezium's ordering of a change, as a comparable pair (MAPPING_SPEC, target schema)."""
    if system == "dublin":
        if source.get("sequence"):
            last_commit, lsn = json.loads(source["sequence"])
            return int(last_commit or 0), int(lsn or source["lsn"])
        return int(source["lsn"]), int(source["lsn"])
    number = int(source["file"].rsplit(".", 1)[1])
    return (number << 32) | int(source["pos"]), int(source["row"])


def ignored_columns() -> dict[str, set[str]]:
    """Columns deployed as known and irrelevant: APPLY_IGNORE_COLUMNS=topic:col,topic:col.

    This is how a schema change is let through after the service stopped on it: someone reads
    the change, decides the spec does not need the column, and says so. Nothing is ignored by
    default, so an unexpected column always stops the service first.
    """
    out: dict[str, set[str]] = defaultdict(set)
    for item in filter(None, os.environ.get("APPLY_IGNORE_COLUMNS", "").split(",")):
        topic, column = item.rsplit(":", 1)
        out[topic].add(column)
    return out


IGNORED = ignored_columns()


def to_change(topic: str, ev, folds: dict) -> Change:
    t = TABLES[topic]
    if topic in IGNORED:
        drop = IGNORED[topic]
        ev = ev.__class__(
            **{
                **ev.__dict__,
                "before": {k: v for k, v in ev.before.items() if k not in drop}
                if ev.before is not None
                else None,
                "after": {k: v for k, v in ev.after.items() if k not in drop}
                if ev.after is not None
                else None,
            }
        )
    image = ev.before if ev.op == "d" else ev.after
    for img in (ev.before, ev.after):
        if img is not None and set(img) != t.columns:
            extra, missing = set(img) - t.columns, t.columns - set(img)
            raise SchemaDrift(f"{topic}: columns added {sorted(extra)}, removed {sorted(missing)}")
    key = image[t.key]
    row_id = tf.target_id(t.system, KEY_TABLE[t.legacy_table], key)
    pos = position(t.system, ev.source)
    legacy = (t.system, t.legacy_table, str(key))
    old_stations: tuple = ()
    new_stations: tuple = ()
    if t.legacy_table == "matka":

        def named(img):
            return tuple(s for s in (img["departure_station_id"], img["return_station_id"]) if s)

        old_stations = named(ev.before) if ev.before is not None else ()
        new_stations = named(ev.after) if ev.after is not None else ()
    if ev.op == "d":
        return Change(t.target_table, row_id, pos, "delete", None, legacy, None, old_stations)
    match t.legacy_table:
        case "station":
            result = tf.dublin_station(ev.after)
        case "station_reading":
            result = tf.dublin_reading(ev.after, folds)
        case "rental":
            result = tf.dublin_rental(ev.after)
        case "asema":
            result = tf.helsinki_station(ev.after)
        case _:
            result = tf.helsinki_trip(ev.after)
    if isinstance(result, tf.Quarantine):
        raw = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in ev.after.items()}
        return Change(
            t.target_table,
            row_id,
            pos,
            "quarantine",
            None,
            legacy,
            (result.rule, result.detail, raw),
            old_stations,
        )
    return Change(
        t.target_table, row_id, pos, "upsert", result, legacy, None, old_stations, new_stations
    )


def newest(changes: list[Change]) -> dict[tuple[str, object], Change]:
    """Per target row, the change with the highest position in this batch."""
    out: dict[tuple[str, object], Change] = {}
    for c in changes:
        k = (c.target_table, c.row_id)
        if k not in out or c.pos > out[k].pos:
            out[k] = c
    return out


@dataclass
class Offsets:
    """Per partition, the offsets read but not yet applied; the next offset to commit is the
    lowest of them, or one past the last read when nothing is pending."""

    pending: dict[tuple[str, int], list[int]] = field(default_factory=lambda: defaultdict(list))
    done: dict[tuple[str, int], set[int]] = field(default_factory=lambda: defaultdict(set))
    last: dict[tuple[str, int], int] = field(default_factory=dict)

    def read(self, tp: tuple[str, int], offset: int) -> None:
        heapq.heappush(self.pending[tp], offset)
        self.last[tp] = max(self.last.get(tp, -1), offset)

    def finished(self, tp: tuple[str, int], offset: int) -> None:
        self.done[tp].add(offset)

    def committable(self) -> dict[tuple[str, int], int]:
        out = {}
        for tp, heap in self.pending.items():
            done = self.done[tp]
            while heap and heap[0] in done:
                done.discard(heapq.heappop(heap))
            out[tp] = heap[0] if heap else self.last[tp] + 1
        return out


# --- the target ---------------------------------------------------------------------------------


def _upsert_sql(table: str) -> str:
    cols = COLUMNS[table]
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != PK[table])
    return (
        f"INSERT INTO fleet.{table} ({', '.join(cols)}) VALUES "
        f"({', '.join(['%s'] * len(cols))}) ON CONFLICT ({PK[table]}) DO UPDATE SET {sets}"
    )


def apply_batch(
    conn: psycopg.Connection,
    changes: list[Change],
    offsets: dict,
    applied_txns: tuple[str, list[str]] = ("", []),
    marker: Marker | None = None,
) -> dict[str, int]:
    """Apply one batch and the offsets it reaches, in one target transaction.

    With a marker, the batch is that marker's transaction and nothing else (the service makes sure
    of it), and the chunk is set aside after the rows: at the marker's position.
    """
    stats: dict[str, int] = defaultdict(int)
    latest = newest(changes)
    with conn.transaction(), conn.cursor() as cur:
        winners: list[Change] = []
        if latest:
            keys = list(latest)
            cur.execute(
                "SELECT target_table, row_id, pos_major, pos_minor FROM migration.row_version "
                "WHERE (target_table, row_id) IN "
                "(SELECT * FROM unnest(%s::text[], %s::uuid[]))",
                ([k[0] for k in keys], [k[1] for k in keys]),
            )
            stored = {(t, r): (a, b) for t, r, a, b in cur.fetchall()}
            for k, c in latest.items():
                if c.pos > stored.get(k, (-1, -1)):
                    winners.append(c)
                else:
                    stats["stale"] += 1

        by = defaultdict(list)
        for c in winners:
            by[(c.target_table, c.kind)].append(c)

        # Stations first, then the stubs trips need, then the rows that reference stations.
        _upsert(cur, "station", by[("station", "upsert")])
        trips = by[("trip", "upsert")]
        refs = sorted({s for c in trips for s in c.new_stations})
        if refs:
            # A station a trip names and nobody has created yet: the stub. An existing station,
            # stub or not, is left as it is.
            cur.executemany(
                _upsert_sql("station").split(" ON CONFLICT")[0] + " ON CONFLICT DO NOTHING",
                [tuple(tf.helsinki_station_stub(s)[k] for k in COLUMNS["station"]) for s in refs],
            )
        _upsert(cur, "station_status", by[("station_status", "upsert")])
        _upsert(cur, "trip", trips)

        # Removals: deletes, and rows that became invalid and go to quarantine.
        for table in ("station_status", "trip"):
            gone = by[(table, "delete")] + by[(table, "quarantine")]
            if gone:
                cur.execute(
                    f"DELETE FROM fleet.{table} WHERE {PK[table]} = ANY(%s)",
                    ([c.row_id for c in gone],),
                )
        _remove_stations(cur, by[("station", "delete")] + by[("station", "quarantine")])

        # Helsinki stubs no trip names any more.
        candidates = {s for c in winners if c.target_table == "trip" for s in c.old_stations}
        if candidates:
            ids = [tf.target_id("helsinki", "asema", s) for s in candidates]
            cur.execute(
                "DELETE FROM fleet.station s "
                "WHERE s.station_id = ANY(%s) AND s.kind = 'unlisted' "
                "AND NOT EXISTS (SELECT 1 FROM fleet.trip t "
                "  WHERE t.departure_station_id = s.station_id) "
                "AND NOT EXISTS (SELECT 1 FROM fleet.trip t "
                "  WHERE t.return_station_id = s.station_id)",
                (ids,),
            )
            stats["stubs_removed"] += cur.rowcount

        quarantined = [c for c in winners if c.kind == "quarantine"]
        if quarantined:
            cur.executemany(
                "INSERT INTO migration.quarantine (source, legacy_table, legacy_key, rule, raw, "
                "pos_major, pos_minor) VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (source, legacy_table, legacy_key) DO UPDATE SET "
                "rule = EXCLUDED.rule, raw = EXCLUDED.raw, "
                "pos_major = EXCLUDED.pos_major, pos_minor = EXCLUDED.pos_minor",
                [
                    (*c.legacy, c.quarantine[0], json.dumps(c.quarantine[2], default=str), *c.pos)
                    for c in quarantined
                ],
            )
        cleared = [c for c in winners if c.kind != "quarantine"]
        if cleared:
            cur.execute(
                "DELETE FROM migration.quarantine WHERE (source, legacy_table, legacy_key) IN "
                "(SELECT * FROM unnest(%s::text[], %s::text[], %s::text[]))",
                (
                    [c.legacy[0] for c in cleared],
                    [c.legacy[1] for c in cleared],
                    [c.legacy[2] for c in cleared],
                ),
            )

        _merge(
            cur,
            "migration.row_version",
            ["target_table", "row_id", "source", "pos_major", "pos_minor", "deleted"],
            [(c.target_table, c.row_id, c.legacy[0], *c.pos, c.kind == "delete") for c in winners],
            ["target_table", "row_id"],
        )
        source, txn_ids = applied_txns
        if marker is not None and set_aside(cur, source, marker):
            stats["markers"] += 1
        if txn_ids:
            cur.executemany(
                "INSERT INTO migration.applied_txn (source, txn_id) VALUES (%s, %s) "
                "ON CONFLICT DO NOTHING",
                [(source, t) for t in txn_ids],
            )
        if offsets:
            cur.executemany(
                "INSERT INTO migration.consumer_offset VALUES (%s, %s, %s) "
                "ON CONFLICT (topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset",
                [(t, p, o) for (t, p), o in offsets.items()],
            )
        for c in winners:
            stats[f"{c.target_table}.{c.kind}"] += 1
    return stats


def _merge(cur, table: str, columns: list[str], rows: list[tuple], key: list[str]) -> None:
    """COPY the rows into a session-local staging table, then one set-based upsert.

    `newest` has already left one change per key, so the INSERT never meets its own key twice.
    """
    if not rows:
        return
    stage = "_stage_" + table.replace(".", "_")
    cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {stage} (LIKE {table}) ON COMMIT DELETE ROWS")
    cols = ", ".join(columns)
    with cur.copy(f"COPY {stage} ({cols}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in key)
    cur.execute(
        f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {stage} "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {updates}"
    )


def _upsert(cur, table: str, changes: list[Change]) -> None:
    _merge(
        cur,
        f"fleet.{table}",
        COLUMNS[table],
        [tuple(c.row[k] for k in COLUMNS[table]) for c in changes],
        [PK[table]],
    )


def _remove_stations(cur, changes: list[Change]) -> None:
    """A deleted station: Helsinki's turns back into a stub while trips name it."""
    for c in changes:
        system, _, key = c.legacy
        if system == "helsinki":
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM fleet.trip WHERE departure_station_id = %(id)s "
                "OR return_station_id = %(id)s)",
                {"id": c.row_id},
            )
            if cur.fetchone()[0]:
                cur.execute(
                    _upsert_sql("station"),
                    tuple(tf.helsinki_station_stub(key)[k] for k in COLUMNS["station"]),
                )
                continue
        cur.execute("DELETE FROM fleet.station WHERE station_id = %s", (c.row_id,))


# --- the consumer -------------------------------------------------------------------------------


@dataclass
class Txn:
    """A source transaction being assembled from its events, which arrive across topics."""

    changes: list[tuple[tuple[str, int], int, Change]] = field(default_factory=list)
    seen: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    expected: dict[str, int] | None = None  # from END, only the collections this service reads
    end_at: tuple[tuple[str, int], int] | None = None
    marker: Marker | None = None
    meta: list[tuple[tuple[str, int], int]] = field(default_factory=list)  # events with no row

    def complete(self) -> bool:
        return self.expected is not None and all(
            self.seen[c] >= n for c, n in self.expected.items()
        )


def txn_key(system: str, debezium_id: str) -> str:
    """The identity of a source transaction across its BEGIN, its events and its END.

    PostgreSQL's Debezium id is `txId:LSN`, and the LSN is not the same in every record: BEGIN
    and the data events carry one, END carries the commit's. Only `txId` identifies the
    transaction. MySQL's id is the GTID, the same everywhere.
    """
    return debezium_id.split(":", 1)[0] if system == "dublin" else debezium_id


def collection_of(topic: str) -> str:
    """'dublin.public.rental' -> 'public.rental', as Debezium names it in transaction records."""
    return topic.split(".", 1)[1]


class Service:
    def __init__(self, system: str, batch: int):
        self.system = system
        self.batch = batch
        self.marker_topic = MARKER_TOPICS[system]
        self.marker_collection = collection_of(self.marker_topic)
        self.data_topics = [*(t for t in TABLES if t.startswith(f"{system}.")), self.marker_topic]
        self.txn_topic = f"{system}.transaction"
        self.collections = {collection_of(t) for t in self.data_topics}
        self.registry = Registry(REGISTRY)
        self.offsets = Offsets()
        self.txns: dict[str, Txn] = {}
        self.ready: list[tuple[tuple[str, int], int, Change]] = []
        self.finished_meta: list[tuple[tuple[str, int], int]] = []
        self.totals: dict[str, int] = defaultdict(int)
        self.applied: set[str] = set()  # transactions already in the target
        self.completed_since_flush: list[str] = []
        self.stop = False
        self.folds: dict = {}
        # Markers whose transaction is complete, waiting for every earlier transaction, in the
        # order of their END records; and the transactions that ended after the first of them.
        self.markers: list[tuple[int, str, Txn]] = []
        self.held: list[tuple[str, Txn]] = []

    def start(self) -> Consumer:
        if self.system == "dublin":
            fm = fold_map.load(LEGACY_A_DSN)
            self.folds = fm.folds
            print(
                f"fold map: {len(fm.folds):,} readings in {len(fm.hours)} autumn hours, "
                f"{fm.stations_without_step} station-hours without a backward step"
            )
        self.target = psycopg.connect(TARGET_DSN)
        # Commits do not wait for the WAL to reach the disk. A crash can lose the last few, but
        # each batch carries its rows and its offsets in one transaction, so they are lost
        # together, the restart reads them again from the earlier offset, and the version guard
        # applies them again. Durability is the source's and Kafka's; the target only has to be
        # consistent, and atomicity is what keeps it so.
        self.target.execute("SET synchronous_commit = off")
        self.target.commit()
        stored = dict(
            ((t, p), o)
            for t, p, o in self.target.execute(
                "SELECT topic, partition, next_offset FROM migration.consumer_offset "
                "WHERE topic LIKE %s",
                (f"{self.system}.%",),
            ).fetchall()
        )
        self.applied = {
            t
            for (t,) in self.target.execute(
                "SELECT txn_id FROM migration.applied_txn WHERE source = %s", (self.system,)
            ).fetchall()
        }
        self.target.commit()
        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA,
                "group.id": f"apply-{self.system}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "fetch.max.bytes": 64 * 1024 * 1024,
            }
        )
        self.stored = stored
        self.assigned: set[tuple[str, int]] = set()
        consumer.assign([])
        self.refresh(consumer)
        print(f"resuming {len(stored)} partitions from the target")
        return consumer

    def refresh(self, consumer: Consumer) -> None:
        """Assign every partition of the topics this service reads, as the topics appear.

        Debezium creates a table's topic with its first event. A service started while a table
        was still empty would otherwise never read it, and every transaction touching that table
        would wait for events it never receives (found on 2026-11-19: the rentals of an emptied
        Dublin, 2,967 transactions open).
        """
        meta = consumer.list_topics(timeout=10).topics
        new = []
        for topic in [*self.data_topics, self.txn_topic]:
            if topic in meta and meta[topic].error is None:
                for p in meta[topic].partitions:
                    if (topic, p) not in self.assigned:
                        self.assigned.add((topic, p))
                        offset = self.stored.get((topic, p), OFFSET_BEGINNING)
                        new.append(TopicPartition(topic, p, offset))
        if new:
            consumer.incremental_assign(new)
            print(f"assigned {[f'{t.topic}/{t.partition}' for t in new]}", flush=True)

    def handle(self, msg) -> None:
        tp, offset = (msg.topic(), msg.partition()), msg.offset()
        self.offsets.read(tp, offset)
        if msg.value() is None:  # a tombstone, if ever enabled: nothing to apply
            self.finished_meta.append((tp, offset))
            return
        record, semantic = self.registry.decode(msg.value())
        if msg.topic() == self.txn_topic:
            txn_id = txn_key(self.system, record["id"])
            if record["status"] == "BEGIN":
                self.finished_meta.append((tp, offset))
                return
            expected = {
                d["data_collection"]: d["event_count"]
                for d in (record.get("data_collections") or [])
                if d["data_collection"] in self.collections
            }
            if not expected:  # a transaction on tables this service does not read
                self.finished_meta.append((tp, offset))
                self.txns.pop(txn_id, None)
                return
            if txn_id in self.applied:  # read again after a restart
                self.finished_meta.append((tp, offset))
                self.txns.pop(txn_id, None)
                return
            txn = self.txns.setdefault(txn_id, Txn())
            txn.expected, txn.end_at = expected, (tp, offset)
            self._maybe_ready(txn_id)
            return
        ev = event(msg.topic(), msg.partition(), offset, record, semantic)
        if msg.topic() == self.marker_topic:
            self._marker_event(tp, offset, ev)
            return
        change = to_change(msg.topic(), ev, self.folds)
        if ev.transaction is None or txn_key(self.system, ev.transaction["id"]) in self.applied:
            # A snapshot read, or an event of a transaction applied before a restart: nothing to
            # wait for, and the version guard decides.
            self.ready.append((tp, offset, change))
            return
        txn_id = txn_key(self.system, ev.transaction["id"])
        txn = self.txns.setdefault(txn_id, Txn())
        txn.changes.append((tp, offset, change))
        txn.seen[collection_of(msg.topic())] += 1
        self._maybe_ready(txn_id)

    def _marker_event(self, tp, offset, ev) -> None:
        """A marker row: inserted by the verifier, or deleted when it cleans up (nothing to do)."""
        if ev.transaction is None or txn_key(self.system, ev.transaction["id"]) in self.applied:
            self.finished_meta.append((tp, offset))
            return
        txn_id = txn_key(self.system, ev.transaction["id"])
        txn = self.txns.setdefault(txn_id, Txn())
        txn.meta.append((tp, offset))
        txn.seen[self.marker_collection] += 1
        if ev.op == "c":
            a = ev.after
            txn.marker = Marker(str(a["id"]), a["legacy_table"], int(a["lo"]), int(a["hi"]))
        self._maybe_ready(txn_id)

    def _maybe_ready(self, txn_id: str) -> None:
        txn = self.txns[txn_id]
        if not txn.complete():
            return
        del self.txns[txn_id]
        end = txn.end_at[1]
        if txn.marker is not None:
            self.markers.append((end, txn_id, txn))
            self.markers.sort(key=lambda m: m[0])
        elif (first := self._first_marker()) is not None and end > first:
            self.held.append((txn_id, txn))  # committed after a marker not yet applied
        else:
            self._release(txn_id, txn)
        self._advance_markers()

    def _first_marker(self) -> int | None:
        """The END offset of the earliest marker not yet applied, complete or not.

        A marker's END names the marker table, so a marker transaction is known as one from its
        END on, before its marker event has arrived.
        """
        ends = [m[0] for m in self.markers[:1]]
        ends += [
            t.end_at[1]
            for t in self.txns.values()
            if t.end_at is not None and self.marker_collection in (t.expected or {})
        ]
        return min(ends) if ends else None

    def _release(self, txn_id: str, txn: Txn) -> None:
        self.ready.extend(txn.changes)
        self.finished_meta.extend([*txn.meta, txn.end_at])
        self.completed_since_flush.append(txn_id)
        self.applied.add(txn_id)

    def _advance_markers(self) -> None:
        """Apply each waiting marker once every transaction that ended before it is applied."""
        while self.markers:
            end, txn_id, txn = self.markers[0]
            # The transaction topic is read in order, so every END before this marker's has been
            # read; one still open here is an earlier transaction missing some of its events.
            if any(t.end_at is not None and t.end_at[1] < end for t in self.txns.values()):
                return
            self.flush()  # everything that committed before the marker
            for tp, offset, _ in txn.changes:
                self.offsets.finished(tp, offset)
            for tp, offset in [*txn.meta, txn.end_at]:
                self.offsets.finished(tp, offset)
            self.applied.add(txn_id)
            stats = apply_batch(
                self.target,
                [c for _, _, c in txn.changes],
                self.offsets.committable(),
                (self.system, [txn_id]),
                marker=txn.marker,
            )
            for k, v in stats.items():
                self.totals[k] += v
            self.markers.pop(0)
            nxt = self._first_marker()
            still = []
            for held_id, held in self.held:
                if nxt is None or held.end_at[1] < nxt:
                    self._release(held_id, held)
                else:
                    still.append((held_id, held))
            self.held = still

    def flush(self) -> None:
        if not self.ready and not self.finished_meta:
            return
        for tp, offset, _ in self.ready:
            self.offsets.finished(tp, offset)
        for tp, offset in self.finished_meta:
            self.offsets.finished(tp, offset)
        stats = apply_batch(
            self.target,
            [c for _, _, c in self.ready],
            self.offsets.committable(),
            (self.system, self.completed_since_flush),
        )
        self.completed_since_flush = []
        for k, v in stats.items():
            self.totals[k] += v
        self.ready, self.finished_meta = [], []

    def run(self) -> None:
        consumer = self.start()
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "stop", True))
        last_report = last_flush = time.monotonic()
        applied_at_report = 0
        try:
            last_refresh = time.monotonic()
            while not self.stop:
                if time.monotonic() - last_refresh > 2:
                    self.refresh(consumer)
                    last_refresh = time.monotonic()
                msgs = consumer.consume(num_messages=self.batch, timeout=0.5)
                for m in msgs:
                    if m.error():
                        raise RuntimeError(m.error())
                    self.handle(m)
                now = time.monotonic()
                if len(self.ready) >= self.batch or (now - last_flush > 1.0):
                    self.flush()
                    last_flush = now
                if now - last_report > 10:
                    applied = sum(v for k, v in self.totals.items() if "." in k)
                    rate = (applied - applied_at_report) / (now - last_report)
                    print(
                        f"{time.strftime('%H:%M:%S')} applied {applied:,} ({rate:,.0f}/s), "
                        f"open transactions {len(self.txns)}, {dict(self.totals)}",
                        flush=True,
                    )
                    last_report, applied_at_report = now, applied
            self.flush()
        finally:
            consumer.close()
            self.target.close()


def main() -> None:
    p = argparse.ArgumentParser(prog="migration.apply")
    p.add_argument("system", choices=["dublin", "helsinki"])
    p.add_argument("--batch", type=int, default=20000)
    args = p.parse_args()
    Service(args.system, args.batch).run()


if __name__ == "__main__":
    main()
