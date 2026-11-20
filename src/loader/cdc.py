"""Register, snapshot and reset the CDC connectors.

python -m loader.cdc register            put both connector configs to Kafka Connect
python -m loader.cdc snapshot <tables>   request an incremental snapshot, e.g. public.station
python -m loader.cdc reset               remove every trace of a previous capture

Reset is the careful one. Deleting a connector does not delete its offsets: a connector registered
again under the same name resumes from the old position, which after a reload points into a
database that no longer exists. So a reset stops each connector, deletes its offsets through the
Connect REST API (KIP-875), deletes the connector, drops PostgreSQL's replication slot and
publication (a slot left behind retains WAL forever), and deletes the CDC topics and the schema
history topic.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import psycopg
import pymysql

ROOT = Path(__file__).resolve().parents[2]
CONNECT = "http://localhost:8083"
CONNECTORS = {"dublin": "public", "helsinki": "helsinki"}
PG = "host=localhost port=5433 dbname=dublin user=dublin password=dublin"


def _http(method: str, path: str, body: dict | None = None) -> tuple[int, dict | None]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        CONNECT + path, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode()
            return resp.status, json.loads(text) if text else None
    except urllib.error.HTTPError as e:
        text = e.read().decode()
        return e.code, json.loads(text) if text else None


def register() -> None:
    for name in CONNECTORS:
        config = json.loads((ROOT / "connect" / f"{name}.json").read_text(encoding="utf8"))
        status, _ = _http("PUT", f"/connectors/{name}/config", config)
        print(f"{name}: config {status}")


def snapshot(tables: list[str]) -> None:
    by_system: dict[str, list[str]] = {}
    for t in tables:
        system = "dublin" if t.startswith("public.") else "helsinki"
        by_system.setdefault(system, []).append(t)
    for system, collections in by_system.items():
        signal = (
            f"snapshot-{uuid.uuid4()}"[:42],
            "execute-snapshot",
            json.dumps({"data-collections": collections, "type": "incremental"}),
        )
        if system == "dublin":
            with psycopg.connect(PG) as conn:
                conn.execute("INSERT INTO debezium_signal VALUES (%s, %s, %s)", signal)
        else:
            conn = pymysql.connect(
                host="127.0.0.1",
                port=3307,
                user="helsinki",
                password="helsinki",
                database="helsinki",
                autocommit=True,
            )
            with conn.cursor() as cur:
                cur.execute("INSERT INTO debezium_signal VALUES (%s, %s, %s)", signal)
            conn.close()
        print(f"{system}: incremental snapshot requested for {', '.join(collections)}")


def reset() -> None:
    _, names = _http("GET", "/connectors")
    reverse = [n for n in names if n.startswith("fleet")]  # what a cutover may leave behind
    for name in [*CONNECTORS, *reverse]:
        status, _ = _http("GET", f"/connectors/{name}")
        if status == 404:
            continue
        _http("PUT", f"/connectors/{name}/stop")
        for _ in range(30):
            _, s = _http("GET", f"/connectors/{name}/status")
            if s and s["connector"]["state"] == "STOPPED":
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"{name} did not stop")
        status, body = _http("DELETE", f"/connectors/{name}/offsets")
        if status != 200:
            raise RuntimeError(f"{name}: offsets not deleted: {status} {body}")
        _http("DELETE", f"/connectors/{name}")
        print(f"{name}: stopped, offsets deleted, connector deleted")

    with psycopg.connect(PG, autocommit=True) as conn:
        slot = "live_migration_dublin"
        if conn.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot,)
        ).fetchone():
            conn.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
            print(f"dublin: replication slot {slot} dropped")
        conn.execute("DROP PUBLICATION IF EXISTS live_migration_dublin")
        conn.execute("TRUNCATE debezium_signal")

    conn = pymysql.connect(
        host="127.0.0.1",
        port=3307,
        user="helsinki",
        password="helsinki",
        database="helsinki",
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE debezium_signal")
    conn.close()

    topics = _kafka("--list").split()
    doomed = [
        t
        for t in topics
        if t.split(".")[0] in {*CONNECTORS, "fleet"} or t == "schema-history.helsinki"
    ]
    for t in doomed:
        _kafka("--delete", "--topic", t)
    print(f"kafka: {len(doomed)} topics deleted")


def _kafka(*args: str) -> str:
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        *args,
    ]
    out = subprocess.run(
        cmd,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "MSYS_NO_PATHCONV": "1"},
    )
    return out.stdout


def main() -> None:
    p = argparse.ArgumentParser(prog="loader.cdc")
    p.add_argument("command", choices=["register", "snapshot", "reset"])
    p.add_argument("tables", nargs="*")
    args = p.parse_args()
    if args.command == "register":
        register()
    elif args.command == "snapshot":
        snapshot(args.tables)
    else:
        reset()


if __name__ == "__main__":
    main()
