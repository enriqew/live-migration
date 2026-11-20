"""A fresh environment per chaos scenario, and the handles a scenario needs to break it.

`Env.reset()` puts everything back to a known state on the development load: the CDC capture
removed, both legacy systems reloaded, the target recreated empty, the connectors registered, the
apply services running and the incremental snapshot taken and applied. Each scenario starts from
there, so no scenario's damage is inherited by the next.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import psycopg
import pymysql
from confluent_kafka import Consumer

from cutover.run import KAFKA, LEGACY_A, TARGET, lag

ROOT = Path(__file__).resolve().parents[2]
ENV = {**os.environ, "MSYS_NO_PATHCONV": "1", "PYTHONUNBUFFERED": "1"}


def sh(*args: str, check: bool = True, timeout: float = 900) -> str:
    out = subprocess.run(
        list(args), cwd=ROOT, env=ENV, capture_output=True, text=True, timeout=timeout
    )
    if check and out.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed: {out.stderr[-2000:]}")
    return out.stdout


def py(*args: str, **kw) -> str:
    return sh(sys.executable, "-m", *args, **kw)


def helsinki():
    return pymysql.connect(
        host="127.0.0.1",
        port=3307,
        user="helsinki",
        password="helsinki",
        database="helsinki",
        autocommit=True,
    )


class Env:
    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.procs: dict[str, subprocess.Popen] = {}

    # --- processes ------------------------------------------------------------------------------

    def start(self, name: str, module: str, *args: str) -> None:
        log = open(self.run_dir / f"{name}.log", "a", encoding="utf8")  # noqa: SIM115
        self.procs[name] = subprocess.Popen(
            [sys.executable, "-u", "-m", module, *args], cwd=ROOT, env=ENV, stdout=log, stderr=log
        )

    def kill(self, name: str) -> None:
        """Abrupt: TerminateProcess on Windows, SIGTERM elsewhere; no clean shutdown runs."""
        proc = self.procs.pop(name, None)
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=30)

    def alive(self, name: str) -> bool:
        return name in self.procs and self.procs[name].poll() is None

    def stop_all(self) -> None:
        for name in list(self.procs):
            self.kill(name)

    def start_apply(self, apply_module: str = "migration.apply") -> None:
        self.start("apply-dublin", apply_module, "dublin")
        self.start("apply-helsinki", apply_module, "helsinki")

    # --- state ----------------------------------------------------------------------------------

    def reset(self, apply_module: str = "migration.apply") -> None:
        self.stop_all()
        self._gateway_to_legacy()
        py("loader.cdc", "reset")
        py("loader", "load", "--config", "conf/dev.toml", "--reset")
        sh("docker", "compose", "rm", "-sf", "target", "unified")
        sh("docker", "volume", "rm", "live-migration_target", check=False)
        sh("docker", "compose", "up", "-d", "--wait", "target", "unified")
        py("loader.cdc", "register")
        self.wait_connectors()
        self.start_apply(apply_module)
        py("loader.cdc", "snapshot", "public.station", "helsinki.asema")
        self.wait_until(lambda: self.count_target("fleet.station") >= 114 + 457, 120, "stations")
        py("loader.cdc", "snapshot", "public.station_reading", "public.rental", "helsinki.matka")
        self.wait_snapshot()

    def _gateway_to_legacy(self) -> None:
        for path, body in (
            ("/admin/fence", {}),
            ("/admin/route", {"dublin": "http://api-a:8000", "helsinki": "http://api-b:8000"}),
            ("/admin/release", {}),
        ):
            req = urllib.request.Request(
                "http://localhost:8000" + path,
                data=json.dumps(body).encode(),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=30).read()
        for port in (8001, 8002):
            req = urllib.request.Request(
                f"http://localhost:{port}/admin/writes?open=true", data=b"", method="POST"
            )
            urllib.request.urlopen(req, timeout=30).read()

    def wait_connectors(self, timeout: float = 120) -> None:
        def running():
            for name in ("dublin", "helsinki"):
                try:
                    with urllib.request.urlopen(
                        f"http://localhost:8083/connectors/{name}/status", timeout=5
                    ) as resp:
                        s = json.load(resp)
                except Exception:
                    return False
                if s["connector"]["state"] != "RUNNING" or any(
                    t["state"] != "RUNNING" for t in s["tasks"]
                ):
                    return False
            return True

        self.wait_until(running, timeout, "connectors running")

    def count_target(self, table: str) -> int:
        with psycopg.connect(TARGET) as t:
            return t.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

    def legacy_counts(self) -> tuple[int, int, int]:
        with psycopg.connect(LEGACY_A) as a:
            readings = a.execute("SELECT count(*) FROM station_reading").fetchone()[0]
            rentals = a.execute("SELECT count(*) FROM rental").fetchone()[0]
        b = helsinki()
        with b.cursor() as cur:
            cur.execute("SELECT count(*) FROM matka")
            trips = cur.fetchone()[0]
        b.close()
        return readings, rentals, trips

    def lag(self) -> int:
        consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "chaos-lag"})
        try:
            with psycopg.connect(TARGET, autocommit=True) as t:
                return lag(consumer, t)
        finally:
            consumer.close()

    def wait_snapshot(self, timeout: float = 900) -> None:
        """Until every legacy row is in the target (or in quarantine) and nothing is behind."""

        def done():
            readings, rentals, trips = self.legacy_counts()
            with psycopg.connect(TARGET) as t:
                got = t.execute(
                    "SELECT (SELECT count(*) FROM fleet.station_status), "
                    "(SELECT count(*) FROM fleet.trip WHERE system_code = 'dublin'), "
                    "(SELECT count(*) FROM fleet.trip WHERE system_code = 'helsinki'), "
                    "(SELECT count(*) FROM migration.quarantine)"
                ).fetchone()
            return (
                got[0] + got[1] + got[2] + got[3] >= readings + rentals + trips and self.lag() == 0
            )

        self.wait_until(done, timeout, "snapshot applied")

    def wait_drained(self, timeout: float = 300) -> None:
        self.wait_until(lambda: self.lag() == 0, timeout, "apply drained")
        time.sleep(2)
        self.wait_until(lambda: self.lag() == 0, timeout, "apply drained")

    @staticmethod
    def wait_until(predicate, timeout: float, what: str, every: float = 2) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(every)
        raise TimeoutError(f"timed out waiting for {what}")

    # --- results --------------------------------------------------------------------------------

    def verify(self) -> dict:
        sh("cmd", "/c", "del", "/q", str(ROOT / "data" / "verify" / "verify.duckdb*"), check=False)
        py("verifier.run", check=False, timeout=1800)
        return json.loads((ROOT / "data" / "verify" / "report.json").read_text(encoding="utf8"))

    def generate(self, seconds: float, rate: float, seed: int, run_id: str) -> dict:
        py(
            "generator.run",
            "--rate",
            str(rate),
            "--seconds",
            str(seconds),
            "--seed",
            str(seed),
            "--run-id",
            run_id,
            timeout=seconds + 600,
        )
        return json.loads((ROOT / "data" / "runs" / run_id / "summary.json").read_text())
