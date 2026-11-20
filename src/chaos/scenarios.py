"""The scenarios. Each returns (declared, observed); the suite compares them exactly.

`declared` is written before the fault is injected, from what the design says must happen.
`observed` is read afterwards from the verifier, the logs and the systems themselves. Where a
scenario needs a fault to have actually bitten (events replayed, a process killed, an alert
raised), that is part of the declaration too, so a fault that silently missed fails the scenario.
"""

from __future__ import annotations

import json
import re
import time

import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition

from chaos.env import ROOT, Env, helsinki, py, sh
from chaos.planted import PLANT_EVERY
from cutover.run import KAFKA, LEGACY_A

CLEAN_TABLE = {"missing": 0, "unexpected": 0, "different": 0, "columns": {}}


def clean() -> dict:
    return {
        "tables": {t: dict(CLEAN_TABLE) for t in ("station", "station_status", "trip")},
        "quarantine": {"only_expected": 0, "only_target": 0},
        "rows_equal": True,
    }


def verified(report: dict) -> dict:
    tables = {
        t: {k: r.get(k) for k in ("missing", "unexpected", "different", "columns")}
        for t, r in report["tables"].items()
    }
    q = report["quarantine"]
    return {
        "tables": tables,
        "quarantine": {"only_expected": q["only_expected"], "only_target": q["only_target"]},
        "rows_equal": all(
            r["expected_rows"] == r["target_rows"] for r in report["tables"].values()
        ),
    }


def stale(env: Env, name: str) -> int:
    """The highest count of stale (already applied) changes an apply service logged."""
    found = [int(n) for n in re.findall(r"'stale': (\d+)", env.log(name))]
    return max(found, default=0)


def accepted(env: Env, run_id: str, kinds: set[str]) -> int:
    return sum(
        1
        for r in env.generator_requests(run_id)
        if r["result"] == "accepted" and r["kind"] in kinds
    )


# --- 1, 2: writes racing the snapshot ------------------------------------------------------------


def s01_writes_race_the_snapshot(env: Env):
    """Rows updated and deleted while the incremental snapshot is reading them (DBLog windows).

    The generator corrects and erases random historic trips and changes station state while the
    chunks are being read. Watermarks must leave the newer value, and a deleted row must not come
    back from a chunk read before the delete.
    """
    run_id = "chaos-01"
    env.reset(during_snapshot=lambda e: e.start_generator(90, 80, 101, run_id))
    env.wait_proc("generator", 900)
    env.wait_drained()
    declared = {**clean(), "historic_updates": True, "historic_deletes": True, "failed": 0}
    s = env.generator_summary(run_id)
    observed = {
        **verified(env.verify()),
        "historic_updates": accepted(env, run_id, {"correct"}) > 0,
        "historic_deletes": accepted(env, run_id, {"erase"}) > 0,
        "failed": s["by_result"].get("failed", 0),
    }
    return declared, observed


# --- 3: apply killed mid-transaction ------------------------------------------------------------


def s03_apply_killed(env: Env):
    """Both apply services killed three times mid-snapshot, with writes going on."""
    run_id = "chaos-03"

    def kill_three_times(e: Env):
        e.start_generator(90, 60, 103, run_id)
        for _ in range(3):
            time.sleep(15)
            e.kill("apply-dublin")
            e.kill("apply-helsinki")
            e.restarts["killed"] = e.restarts.get("killed", 0) + 1
            e.start_apply()

    env.reset(during_snapshot=kill_three_times)
    env.wait_proc("generator", 900)
    env.wait_drained()
    declared = {**clean(), "kills": 3, "replayed_as_stale": True}
    observed = {
        **verified(env.verify()),
        "kills": env.restarts.get("killed", 0),
        "replayed_as_stale": stale(env, "apply-dublin") + stale(env, "apply-helsinki") > 0,
    }
    return declared, observed


# --- 4: connector rewound -----------------------------------------------------------------------


def s04_connector_rewound(env: Env):
    """The Helsinki connector stopped and restarted from an offset 30 seconds in the past."""
    run_id = "chaos-04"
    env.reset()
    _, saved = env.connect("GET", "/connectors/helsinki/offsets")
    env.generate(30, 40, 104, run_id)
    env.wait_drained()
    env.connect("PUT", "/connectors/helsinki/stop")
    env.wait_until(
        lambda: (
            env.connect("GET", "/connectors/helsinki/status")[1]["connector"]["state"] == "STOPPED"
        ),
        60,
        "connector stopped",
    )
    status, body = env.connect("PATCH", "/connectors/helsinki/offsets", saved)
    if status != 200:
        raise RuntimeError(f"offsets not rewound: {status} {body}")
    before = stale(env, "apply-helsinki")
    env.connect("PUT", "/connectors/helsinki/resume")
    env.wait_connectors(("helsinki",))
    time.sleep(20)
    env.wait_drained()
    declared = {**clean(), "replayed_as_stale": True}
    observed = {
        **verified(env.verify()),
        "replayed_as_stale": stale(env, "apply-helsinki") > before,
    }
    return declared, observed


# --- 5: duplicated and reordered events ---------------------------------------------------------


def _republish_reversed(topic: str, count: int) -> int:
    """Copy the last `count` messages of a topic onto its end again, in reverse order."""
    consumer = Consumer(
        {"bootstrap.servers": KAFKA, "group.id": "chaos-dup", "enable.auto.commit": False}
    )
    low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
    start = max(low, high - count)
    consumer.assign([TopicPartition(topic, 0, start)])
    msgs = []
    while len(msgs) < high - start:
        m = consumer.poll(5)
        if m is None:
            break
        if not m.error():
            msgs.append(m)
    consumer.close()
    producer = Producer({"bootstrap.servers": KAFKA})
    for m in reversed(msgs):
        producer.produce(topic, key=m.key(), value=m.value(), headers=m.headers())
    producer.flush(30)
    return len(msgs)


def s05_duplicates_reordered(env: Env):
    """The last 2,000 events of each data topic delivered again, newest first."""
    run_id = "chaos-05"
    env.reset()
    env.generate(30, 40, 105, run_id)
    env.wait_drained()
    before = stale(env, "apply-dublin") + stale(env, "apply-helsinki")
    copied = sum(
        _republish_reversed(t, 2000)
        for t in (
            "dublin.public.station_reading",
            "dublin.public.rental",
            "helsinki.helsinki.matka",
        )
    )
    time.sleep(10)
    env.wait_drained()
    declared = {**clean(), "republished": True, "all_stale": True}
    after = stale(env, "apply-dublin") + stale(env, "apply-helsinki")
    observed = {
        **verified(env.verify()),
        "republished": copied > 0,
        "all_stale": after - before >= copied * 0.99,
    }
    return declared, observed


# --- 6: schema change mid-run -------------------------------------------------------------------


def s06_schema_drift(env: Env):
    """A column added to `matka` mid-run: the service stops, then resumes once it is mapped."""
    run_id = "chaos-06"
    env.reset()
    b = helsinki()
    try:
        with b.cursor() as cur:
            cur.execute("ALTER TABLE matka ADD COLUMN note varchar(20) NULL")
        env.unsupervise("apply-helsinki")  # a stopped service stays stopped until someone acts
        env.generate(20, 30, 106, run_id)
        env.wait_until(lambda: not env.alive("apply-helsinki"), 120, "the service to stop")
        stopped_on_drift = "SchemaDrift" in env.log(
            "apply-helsinki"
        ) and "columns added ['note']" in env.log("apply-helsinki")
        dublin_kept_going = env.alive("apply-dublin")
        # The mapping lands: the column is known and irrelevant to the spec.
        env.start(
            "apply-helsinki",
            "migration.apply",
            "helsinki",
            supervise=True,
            env={"APPLY_IGNORE_COLUMNS": "helsinki.helsinki.matka:note"},
        )
        env.wait_drained()
        declared = {**clean(), "stopped_on_drift": True, "dublin_kept_going": True}
        observed = {
            **verified(env.verify()),
            "stopped_on_drift": stopped_on_drift,
            "dublin_kept_going": dublin_kept_going,
        }
        return declared, observed
    finally:
        with b.cursor() as cur:
            cur.execute("ALTER TABLE matka DROP COLUMN note")
        b.close()


# --- 7: connector down, WAL retained ------------------------------------------------------------


def retained_bytes() -> int:
    with psycopg.connect(LEGACY_A) as a:
        return int(
            a.execute(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) "
                "FROM pg_replication_slots WHERE slot_name = 'live_migration_dublin'"
            ).fetchone()[0]
        )


ALERT_BYTES = 1 << 20  # 1 MiB: a dev load, where the real limit is max_slot_wal_keep_size=4GB


def s07_connector_down(env: Env):
    """The Dublin connector paused while writes go on: the slot retains WAL, the alert fires
    before the limit, and once resumed nothing is lost."""
    run_id = "chaos-07"
    env.reset()
    env.connect("PUT", "/connectors/dublin/pause")
    env.wait_until(
        lambda: (
            env.connect("GET", "/connectors/dublin/status")[1]["connector"]["state"] == "PAUSED"
        ),
        60,
        "connector paused",
    )
    start = retained_bytes()
    env.generate(40, 40, 107, run_id)
    peak = retained_bytes()
    env.connect("PUT", "/connectors/dublin/resume")
    env.wait_connectors(("dublin",))
    time.sleep(15)
    env.wait_drained()
    declared = {**clean(), "retained_grew": True, "alert_fired": True}
    observed = {
        **verified(env.verify()),
        "retained_grew": peak > start,
        "alert_fired": peak >= ALERT_BYTES,
    }
    return declared, observed


# --- 8: target restarted mid-apply --------------------------------------------------------------


def s08_target_restart(env: Env):
    """The target restarted under the apply services mid-snapshot: they fail, are restarted by
    the supervisor, and resume from the offsets the target kept."""
    run_id = "chaos-08"

    def restart_target(e: Env):
        e.start_generator(60, 40, 108, run_id)
        time.sleep(20)
        sh("docker", "compose", "restart", "target")

    env.reset(during_snapshot=restart_target)
    env.wait_proc("generator", 900)
    env.wait_drained()
    declared = {**clean(), "services_restarted": True}
    observed = {**verified(env.verify()), "services_restarted": sum(env.restarts.values()) >= 1}
    return declared, observed


# --- 9: cutover, writes, rollback ---------------------------------------------------------------


def s09_cutover_and_rollback(env: Env):
    """The whole cycle under load: no write fails, each pause stays under 5 s, nothing is lost."""
    run_id = "chaos-09"
    env.reset()
    env.start_generator(180, 50, 109, run_id)
    time.sleep(30)
    cut = py("cutover.run", check=False)
    cut_ok = '"ok": true' in cut.splitlines()[-1]
    if cut_ok:
        env.start("reverse", "migration.reverse")
        time.sleep(70)
        back = py("cutover.rollback", check=False)
        back_ok = '"ok": true' in back.splitlines()[-1]
        env.kill("reverse")
    else:
        back_ok = False
    env.wait_proc("generator", 900)
    env.wait_drained()
    s = env.generator_summary(run_id)
    pauses = []
    for kind in ("cutover", "rollback"):
        runs = sorted((ROOT / "data" / "runs").glob(f"{kind}-*/{kind}.json"))
        if runs:
            steps = json.loads(runs[-1].read_text())["steps"]
            pauses += [st.get("fenced_ms", 0) for st in steps if st["step"] == "released"]
    declared = {
        **clean(),
        "cutover_ok": True,
        "rollback_ok": True,
        "failed": 0,
        "pauses_under_5s": True,
    }
    observed = {
        **verified(env.verify()),
        "cutover_ok": cut_ok,
        "rollback_ok": back_ok,
        "failed": s["by_result"].get("failed", 0),
        "pauses_under_5s": len(pauses) == 2 and all(p < 5000 for p in pauses),
    }
    return declared, observed


# --- 10: a bug planted in the transform ---------------------------------------------------------


def s10_planted_bug(env: Env):
    """A unit bug in the transform: the verifier must name those rows and that column only."""
    env.reset("chaos.planted")
    b = helsinki()
    with b.cursor() as cur:
        # The planted rows that show: distance known, not negative (that becomes null), not zero.
        cur.execute(
            f"SELECT count(*) FROM matka WHERE id % {PLANT_EVERY} = 0 AND covered_distance_m > 0"
        )
        n = cur.fetchone()[0]
    b.close()
    declared = clean()
    declared["tables"]["trip"] = {
        "missing": 0,
        "unexpected": 0,
        "different": n,
        "columns": {"distance_m": n},
    }
    return declared, verified(env.verify())


SCENARIOS = {
    "01-writes-race-the-snapshot": s01_writes_race_the_snapshot,
    "03-apply-killed": s03_apply_killed,
    "04-connector-rewound": s04_connector_rewound,
    "05-duplicates-reordered": s05_duplicates_reordered,
    "06-schema-drift": s06_schema_drift,
    "07-connector-down": s07_connector_down,
    "08-target-restart": s08_target_restart,
    "09-cutover-and-rollback": s09_cutover_and_rollback,
    "10-planted-bug": s10_planted_bug,
}
