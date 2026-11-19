"""The rollback: python -m cutover.rollback

Back to the legacy systems after a cutover, losing nothing written in between. Timed and gated
like the cutover, and written to data/runs/rollback-<ts>/rollback.json.

1. fence: the gateway holds new requests and drains the ones in flight to the unified API;
2. mark: a row in `migration.rollback_marker`, captured by the reverse connector; once it is in
   Kafka, everything the unified API committed before the fence is in Kafka too;
3. drain: the reverse replication commits past the end offsets of its topics as they stand then;
4. stop the reverse path: its connector is deleted and its slot dropped, so nothing it would carry
   can come back round once the forward path runs again;
5. hand back: the unified API's recent idempotency keys go to each legacy API, and each legacy
   sequence moves past the keys the unified API created;
6. resume the forward connectors: they carry the reverse replication's own writes to the target,
   where they change nothing, since the target already holds those values;
7. route and release: the gateway sends each system to its legacy API again.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime

import psycopg
from confluent_kafka import Consumer, TopicPartition

from cutover.run import (
    KAFKA,
    LEGACY_A,
    REGISTRY,
    ROOT,
    TARGET,
    TOPICS,
    Timeline,
    connect_api,
    helsinki,
    post,
)
from migration.decode import Registry

REVERSE_TOPICS = ["fleet.fleet.station", "fleet.fleet.station_status", "fleet.fleet.trip"]
MARKER_TOPIC = "fleet.migration.rollback_marker"
LEGACY = {"dublin": "http://api-a:8000", "helsinki": "http://api-b:8000"}


def await_marker(marker: str, timeout_s: float = 60) -> None:
    registry = Registry(REGISTRY)
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA,
            "group.id": f"rollback-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:  # the topic appears with the first marker ever written
        if MARKER_TOPIC in consumer.list_topics(timeout=5).topics:
            break
        time.sleep(0.1)
    consumer.assign([TopicPartition(MARKER_TOPIC, 0, 0)])
    while time.monotonic() < deadline:
        msg = consumer.poll(0.2)
        if msg is None or msg.error() or msg.value() is None:
            continue
        record, _ = registry.decode(msg.value())
        if (record.get("after") or {}).get("id") == marker:
            consumer.close()
            return
    consumer.close()
    raise TimeoutError("rollback marker not seen in Kafka")


def reverse_behind(consumer: Consumer, target) -> dict:
    ends = {}
    meta = consumer.list_topics(timeout=10).topics
    for topic in REVERSE_TOPICS:
        if topic in meta:
            for p in meta[topic].partitions:
                ends[(topic, p)] = consumer.get_watermark_offsets(
                    TopicPartition(topic, p), timeout=10
                )[1]
    done = {
        (t, p): o
        for t, p, o in target.execute(
            "SELECT topic, partition, next_offset FROM migration.reverse_offset"
        ).fetchall()
    }
    return {
        f"{t}/{p}": h - done.get((t, p), 0) for (t, p), h in ends.items() if done.get((t, p), 0) < h
    }


def hand_back(target) -> dict:
    keys = target.execute(
        "SELECT key, request_hash, response::text, created_at FROM migration.api_idempotency "
        "WHERE created_at > now() - interval '1 hour'"
    ).fetchall()
    with psycopg.connect(LEGACY_A) as a:
        a.cursor().executemany(
            "INSERT INTO api_idempotency (key, request_hash, response, created_at) "
            "VALUES (%s, %s, %s::jsonb, %s) ON CONFLICT (key) DO NOTHING",
            keys,
        )
        a.execute(
            "SELECT setval(pg_get_serial_sequence('rental', 'rental_id'), "
            "(SELECT coalesce(max(rental_id), 1) FROM rental))"
        )
        a.execute(
            "SELECT setval(pg_get_serial_sequence('station_reading', 'reading_id'), "
            "(SELECT coalesce(max(reading_id), 1) FROM station_reading))"
        )
    b = helsinki()
    with b.cursor() as cur:
        cur.executemany(
            "INSERT IGNORE INTO api_idempotency (`key`, request_hash, response, created_at) "
            "VALUES (%s, %s, %s, %s)",
            keys,
        )
        cur.execute("SELECT coalesce(max(id), 0) + 1 FROM matka")
        next_id = int(cur.fetchone()[0])
        cur.execute(f"ALTER TABLE matka AUTO_INCREMENT = {next_id}")
    b.close()
    return {"keys_copied": len(keys), "next_matka_id": next_id}


def stop_reverse(target) -> None:
    connect_api("DELETE", "/connectors/fleet")
    for _ in range(200):
        exists = target.execute(
            "SELECT active FROM pg_replication_slots WHERE slot_name = 'reverse_fleet'"
        ).fetchone()
        if exists is None:
            return
        if not exists[0]:
            target.execute("SELECT pg_drop_replication_slot('reverse_fleet')")
            return
        time.sleep(0.05)
    raise TimeoutError("reverse slot still active")


def main() -> None:
    run_dir = ROOT / "data" / "runs" / f"rollback-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    run_dir.mkdir(parents=True, exist_ok=True)
    tl = Timeline()
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "rollback-watermarks"})
    target = psycopg.connect(TARGET, autocommit=True)
    result = {"ok": False}
    try:
        fence = post("/admin/fence")
        tl.step("fence", **fence)

        marker = f"rollback-{uuid.uuid4()}"
        target.execute("INSERT INTO migration.rollback_marker (id) VALUES (%s)", (marker,))
        await_marker(marker)
        tl.step("marked")

        deadline = time.monotonic() + 120
        while behind := reverse_behind(consumer, target):
            if time.monotonic() > deadline:
                raise TimeoutError(f"reverse replication did not drain: {behind}")
            time.sleep(0.05)
        tl.step("drained")

        stop_reverse(target)
        tl.step("reverse_stopped")

        handed = hand_back(target)
        tl.step("handed_back", **handed)

        for name in TOPICS:
            connect_api("PUT", f"/connectors/{name}/resume")
        tl.step("forward_resumed")

        post("/admin/route", LEGACY)
        released = post("/admin/release")
        tl.step("released", **released)
        result = {"ok": True}
    except Exception as e:
        result = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        try:
            released = post("/admin/release")  # still on the unified API: nothing was lost
            tl.step("aborted_and_released_to_unified", **released)
        except Exception as e2:  # noqa: BLE001
            tl.step("release_failed", error=str(e2))
    finally:
        consumer.close()
        target.close()
    report = {"started_at": tl.started_at, **result, "steps": tl.steps}
    (run_dir / "rollback.json").write_text(
        json.dumps(report, indent=2, default=str) + "\n", encoding="utf8"
    )
    print(json.dumps(result))


if __name__ == "__main__":
    main()
