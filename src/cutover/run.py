"""The cutover: python -m cutover.run [--max-lag 2000]

Every step is timed and gated, and the timeline is written to data/runs/cutover-<ts>/cutover.json.

1. gate: the apply services are within `--max-lag` messages of every CDC topic;
2. fence: the gateway holds new requests and waits for the in-flight ones; from here on nothing
   reaches the legacy APIs;
3. mark: a marker row is written into each legacy system's signal table, a captured table, in its
   own transaction. Debezium emits in commit order, so once the marker is in Kafka, everything the
   legacy system committed before the fence is in Kafka too;
4. drain: the end offsets of every CDC topic are read at that moment, and the cutover waits until
   the apply services have committed past them in the target;
5. reverse: the forward connectors are paused, so the legacy systems' changes stop flowing to
   the target, and the reverse replication's slot is created on the target, so it starts at
   exactly this point and carries only what the unified API writes from here on. The reverse
   connector itself is registered after the release: it reads the slot from where it was made;
6. close the legacy APIs to writes: anything that reaches them from now on, past the gateway,
   is answered 503 and retried by its client through the gateway;
7. hand over: the legacy APIs' recent idempotency keys are copied into the target, and the target's
   key sequences are set past each legacy maximum;
8. route and release: the gateway sends each system to the unified API and lets the held requests
   through.

There is no row count inside the fence. The first version had one, and counting 49 million rows
took 19 seconds of a 20.7-second pause; the 818 requests held behind it then arrived at once and
took the client down with timeouts. Having applied everything up to the marker is the guarantee
the switch needs, and it is checked inside the fence; comparing the data is the verifier's job,
outside it.
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pymysql
from confluent_kafka import Consumer, TopicPartition

from loader.cdc import _http as connect_api
from migration.decode import Registry

ROOT = Path(__file__).resolve().parents[2]
GATEWAY = "http://localhost:8000"
KAFKA = "localhost:29092"
REGISTRY = "http://localhost:8081/apis/registry/v3"
LEGACY_A = "host=localhost port=5433 dbname=dublin user=dublin password=dublin"
TARGET = "host=localhost port=5434 dbname=fleet user=fleet password=fleet"
TOPICS = {
    "dublin": [
        "dublin.public.station",
        "dublin.public.station_reading",
        "dublin.public.rental",
        "dublin.transaction",
    ],
    "helsinki": ["helsinki.helsinki.asema", "helsinki.helsinki.matka", "helsinki.transaction"],
}
SIGNAL_TOPICS = {
    "dublin": "dublin.public.debezium_signal",
    "helsinki": "helsinki.helsinki.debezium_signal",
}
LEGACY_APIS = {"dublin": "http://localhost:8001", "helsinki": "http://localhost:8002"}
UNIFIED = {"dublin": "http://unified:8000/dublin", "helsinki": "http://unified:8000/helsinki"}


def helsinki():
    return pymysql.connect(
        host="127.0.0.1",
        port=3307,
        user="helsinki",
        password="helsinki",
        database="helsinki",
        autocommit=True,
    )


def post(path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(
        GATEWAY + path,
        data=json.dumps(body or {}).encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.load(resp)


class Timeline:
    def __init__(self):
        self.t0 = time.monotonic()
        self.steps: list[dict] = []
        self.started_at = datetime.now(UTC).isoformat()

    def step(self, name: str, **detail):
        self.steps.append(
            {"step": name, "at_ms": round((time.monotonic() - self.t0) * 1000, 1), **detail}
        )
        print(
            f"{self.steps[-1]['at_ms']:>10.1f} ms  {name}  {detail if detail else ''}", flush=True
        )


def end_offsets(consumer: Consumer) -> dict[tuple[str, int], int]:
    out = {}
    for topic in [t for ts in TOPICS.values() for t in ts]:
        meta = consumer.list_topics(topic, timeout=10).topics[topic]
        for p in meta.partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            out[(topic, p)] = high
    return out


def committed(target) -> dict[tuple[str, int], int]:
    return {
        (t, p): o
        for t, p, o in target.execute(
            "SELECT topic, partition, next_offset FROM migration.consumer_offset"
        ).fetchall()
    }


def lag(consumer, target) -> int:
    ends, done = end_offsets(consumer), committed(target)
    return sum(max(0, high - done.get(tp, 0)) for tp, high in ends.items())


def write_markers() -> dict[str, str]:
    markers = {s: f"cutover-{uuid.uuid4()}"[:42] for s in TOPICS}
    with psycopg.connect(LEGACY_A, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO debezium_signal VALUES (%s, 'cutover-marker', NULL)", (markers["dublin"],)
        )
    conn = helsinki()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO debezium_signal VALUES (%s, 'cutover-marker', NULL)",
            (markers["helsinki"],),
        )
    conn.close()
    return markers


def await_markers(markers: dict[str, str], timeout_s: float = 10) -> None:
    registry = Registry(REGISTRY)
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA,
            "group.id": f"cutover-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    pending = dict(markers)
    assignment = []
    for topic in SIGNAL_TOPICS.values():
        meta = consumer.list_topics(topic, timeout=10).topics[topic]
        for p in meta.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            assignment.append(TopicPartition(topic, p, max(low, high - 50)))
    consumer.assign(assignment)
    deadline = time.monotonic() + timeout_s
    while pending and time.monotonic() < deadline:
        msg = consumer.poll(0.2)
        if msg is None or msg.error() or msg.value() is None:
            continue
        record, _ = registry.decode(msg.value())
        after = record.get("after") or {}
        for system, marker in list(pending.items()):
            if after.get("id") == marker:
                del pending[system]
    consumer.close()
    if pending:
        raise TimeoutError(f"markers not seen in Kafka: {pending}")


def legacy_writes(open_: bool) -> None:
    """Open or close the legacy APIs' write gate (legacy_api.common.WriteGate)."""
    for url in LEGACY_APIS.values():
        req = urllib.request.Request(
            f"{url}/admin/writes?open={str(open_).lower()}", data=b"", method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            assert json.load(resp)["writes_open"] is open_


def pause_forward() -> None:
    for name in TOPICS:
        connect_api("PUT", f"/connectors/{name}/pause")
    for _ in range(100):
        states = [
            connect_api("GET", f"/connectors/{n}/status")[1]["connector"]["state"] for n in TOPICS
        ]
        if all(s == "PAUSED" for s in states):
            return
        time.sleep(0.05)
    raise TimeoutError("forward connectors did not pause")


def create_reverse_slot(target) -> str:
    lsn = target.execute(
        "SELECT lsn FROM pg_create_logical_replication_slot('reverse_fleet', 'pgoutput')"
    ).fetchone()[0]
    return str(lsn)


def register_reverse() -> int:
    config = json.loads((ROOT / "connect" / "fleet.json").read_text(encoding="utf8"))
    return connect_api("PUT", "/connectors/fleet/config", config)[0]


def hand_over(target) -> dict:
    """Recent idempotency keys and the key sequences, legacy to target."""
    with psycopg.connect(LEGACY_A) as a:
        keys_a = a.execute(
            "SELECT key, request_hash, response::text, created_at FROM "
            "api_idempotency WHERE created_at > now() - interval '1 hour'"
        ).fetchall()
        max_rental = a.execute("SELECT coalesce(max(rental_id), 0) FROM rental").fetchone()[0]
        max_reading = a.execute(
            "SELECT coalesce(max(reading_id), 0) FROM station_reading"
        ).fetchone()[0]
    b = helsinki()
    with b.cursor() as cur:
        cur.execute(
            "SELECT `key`, request_hash, CAST(response AS CHAR), created_at FROM "
            "api_idempotency WHERE created_at > now() - interval 1 hour"
        )
        keys_b = cur.fetchall()
        cur.execute("SELECT coalesce(max(id), 0) FROM matka")
        max_matka = cur.fetchone()[0]
    b.close()
    with target.transaction():
        target.cursor().executemany(
            "INSERT INTO migration.api_idempotency (key, request_hash, response, created_at) "
            "VALUES (%s, %s, %s::jsonb, %s) ON CONFLICT (key) DO NOTHING",
            [*keys_a, *keys_b],
        )
        for seq, value in (
            ("seq_dublin_rental", max_rental),
            ("seq_dublin_reading", max_reading),
            ("seq_helsinki_matka", max_matka),
        ):
            target.execute(f"SELECT setval('migration.{seq}', %s)", (max(value, 1),))
    return {
        "keys_copied": len(keys_a) + len(keys_b),
        "max_rental": max_rental,
        "max_reading": max_reading,
        "max_matka": max_matka,
    }


def main() -> None:
    p = argparse.ArgumentParser(prog="cutover.run")
    p.add_argument("--max-lag", type=int, default=2000)
    p.add_argument("--drain-timeout", type=float, default=120)
    args = p.parse_args()
    run_dir = ROOT / "data" / "runs" / f"cutover-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    run_dir.mkdir(parents=True, exist_ok=True)
    tl = Timeline()
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "cutover-watermarks"})
    target = psycopg.connect(TARGET, autocommit=True)
    result = {"ok": False}
    fenced = False
    try:
        if target.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'reverse_fleet'"
        ).fetchone():
            raise RuntimeError("a reverse slot already exists: clean up the previous run first")
        current = lag(consumer, target)
        tl.step("gate.lag", messages=current)
        if current > args.max_lag:
            raise RuntimeError(f"lag {current} above {args.max_lag}: not cutting over")

        fence = post("/admin/fence")
        fenced = True
        tl.step("fence", **fence)

        markers = write_markers()
        await_markers(markers)
        tl.step("marked")

        ends = end_offsets(consumer)
        deadline = time.monotonic() + args.drain_timeout
        while True:
            done = committed(target)
            behind = {
                f"{t}/{p}": h - done.get((t, p), 0)
                for (t, p), h in ends.items()
                if done.get((t, p), 0) < h
            }
            if not behind:
                break
            if time.monotonic() > deadline:
                raise TimeoutError(f"apply did not drain: {behind}")
            time.sleep(0.05)
        tl.step("drained")

        pause_forward()
        slot_lsn = create_reverse_slot(target)
        tl.step("reverse_ready", slot_lsn=slot_lsn)

        legacy_writes(False)
        tl.step("legacy_closed")

        handed = hand_over(target)
        tl.step("handed_over", **handed)

        post("/admin/route", UNIFIED)
        released = post("/admin/release")
        tl.step("released", **released)
        tl.step("reverse_registered", status=register_reverse())
        result = {"ok": True}
    except Exception as e:
        # Never leave clients held: on any failure the fence opens on the legacy upstreams.
        result = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        try:
            # Undo what the reverse step did, if it got that far, before letting clients go.
            for name in TOPICS:
                connect_api("PUT", f"/connectors/{name}/resume")
            target.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = 'reverse_fleet' AND NOT active"
            )
            legacy_writes(True)
            if fenced:
                post(
                    "/admin/route", {"dublin": "http://api-a:8000", "helsinki": "http://api-b:8000"}
                )
        except Exception as e3:  # noqa: BLE001
            tl.step("undo_failed", error=str(e3))
        try:
            released = post("/admin/release")
            tl.step("aborted_and_released_to_legacy", **released)
        except Exception as e2:  # noqa: BLE001
            tl.step("release_failed", error=str(e2))
    finally:
        consumer.close()
        target.close()
    report = {"started_at": tl.started_at, **result, "steps": tl.steps}
    (run_dir / "cutover.json").write_text(
        json.dumps(report, indent=2, default=str) + "\n", encoding="utf8"
    )
    print(json.dumps(result))


if __name__ == "__main__":
    main()
