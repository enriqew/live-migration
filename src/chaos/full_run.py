"""The full-size run, end to end from a clean start: python -m chaos.full_run

This is the run the published figures come from. Every phase is timed and its evidence kept in
data/runs/full-<ts>/run.json:

1. load: both legacy systems reloaded from the locked files (conf/load.toml, ~49 million rows);
2. snapshot: the incremental snapshot of every table, with the generator writing to both legacy
   systems the whole time, in 10-minute segments so that each segment's log is complete;
3. cycle: the generator at 50 operations a second for 15 minutes through the gateway; the
   cutover after 1 minute, the rollback after 10, the reverse replication running in between;
4. drain and verify: the generator stops, the forward path catches up, and the verifier
   compares every row of the legacy systems with the target.

`--cycle-only` skips 1 and 2: it runs the cycle and the verification on the state a full run left,
loaded, snapshotted and drained, once the apply services have caught up with whatever is left.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from datetime import UTC, datetime

from chaos.env import ROOT, Env, py

SNAPSHOT_RATE = 20
CYCLE_RATE = 50
CYCLE_SECONDS = 900
CUTOVER_AT = 60
ROLLBACK_AT = 600


def main() -> None:
    p = argparse.ArgumentParser(prog="chaos.full_run")
    p.add_argument("--cycle-only", action="store_true", help="skip the load and the snapshot")
    cycle_only = p.parse_args().cycle_only
    stamp = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    run_dir = ROOT / "data" / "runs" / f"{'cycle' if cycle_only else 'full'}-{stamp}"
    env = Env(run_dir)
    report: dict = {
        "started_at": datetime.now(UTC).isoformat(),
        "cycle_only": cycle_only,
        "segments": [],
    }
    stop_segments = threading.Event()

    def segments() -> None:
        i = 0
        while not stop_segments.is_set():
            run_id = f"full-{stamp}-snapshot-{i:02d}"
            seconds = 600
            env.start(
                "generator",
                "generator.run",
                "--rate",
                str(SNAPSHOT_RATE),
                "--seconds",
                str(seconds),
                "--seed",
                str(1000 + i),
                "--run-id",
                run_id,
            )
            env.procs["generator"].wait()
            env.procs.pop("generator", None)
            report["segments"].append(run_id)
            i += 1

    writer = threading.Thread(target=segments, daemon=True)

    def during_snapshot(e: Env) -> None:
        writer.start()

    try:
        if cycle_only:
            env.start_apply()
            env.wait_drained(600)
        else:
            env.reset(
                config="conf/load.toml",
                during_snapshot=during_snapshot,
                snapshot_timeout=4 * 3600,
                snapshot_max_lag=2000,  # writes continue throughout; the final drain asks for 0
            )
            stop_segments.set()
            writer.join(timeout=700)  # the segment in progress finishes and keeps its log
            env.wait_drained(600)
        report["snapshot_drained_at"] = time.time()

        cycle_id = f"full-{stamp}-cycle"
        env.start(
            "cycle-generator",
            "generator.run",
            "--rate",
            str(CYCLE_RATE),
            "--seconds",
            str(CYCLE_SECONDS),
            "--seed",
            "2000",
            "--run-id",
            cycle_id,
        )
        t0 = time.monotonic()
        time.sleep(CUTOVER_AT)
        report["cutover_stdout"] = py("cutover.run", check=False).splitlines()[-1]
        env.start("reverse", "migration.reverse")
        time.sleep(max(0, ROLLBACK_AT - (time.monotonic() - t0)))
        report["rollback_stdout"] = py("cutover.rollback", check=False).splitlines()[-1]
        env.kill("reverse")
        env.procs["cycle-generator"].wait()
        env.procs.pop("cycle-generator", None)
        report["cycle"] = cycle_id
        env.wait_drained(900)
        report["drained_at"] = time.time()
        env.stop_all()

        for p in (ROOT / "data" / "verify").glob("verify.duckdb*"):
            p.unlink()
        report["verify_started_at"] = time.time()
        py("verifier.run", check=False, timeout=4 * 3600)
        report["verify_finished_at"] = time.time()
        report["verify"] = json.loads((ROOT / "data" / "verify" / "report.json").read_text())
        for kind in ("cutover", "rollback"):
            last = sorted((ROOT / "data" / "runs").glob(f"{kind}-*/{kind}.json"))[-1]
            report[kind] = json.loads(last.read_text())
        report["generator"] = {
            rid: json.loads((ROOT / "data" / "runs" / rid / "summary.json").read_text())
            for rid in [*report["segments"], cycle_id]
        }
        report["ok"] = (
            report["verify"]["clean"] and report["cutover"]["ok"] and report["rollback"]["ok"]
        )
    except Exception as e:  # noqa: BLE001
        report.update(ok=False, error=f"{type(e).__name__}: {e}")
    finally:
        stop_segments.set()
        env.stop_all()
        report["timings"] = env.timings
        report["restarts"] = env.restarts
        report["finished_at"] = datetime.now(UTC).isoformat()
        (run_dir / "run.json").write_text(
            json.dumps(report, indent=2, default=str) + "\n", encoding="utf8"
        )
        print(
            json.dumps({"ok": report.get("ok"), "error": report.get("error"), "run": str(run_dir)}),
            flush=True,
        )


if __name__ == "__main__":
    main()
