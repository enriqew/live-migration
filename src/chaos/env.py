"""A fresh environment per chaos scenario, and the handles a scenario needs to break it.

`Env.reset()` puts everything back to a known state on the development load: the CDC capture
removed, both legacy systems reloaded, the target recreated empty, the connectors registered, the
apply services running and the incremental snapshot taken and applied. Each scenario starts from
there, so no scenario's damage is inherited by the next.

The apply services run supervised: a supervisor restarts one that dies, as an orchestrator would
in production, and counts the restarts so a scenario can say how many it expected.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
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


def http(method: str, url: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else (b"" if method != "GET" else None)
    req = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            text = resp.read().decode()
            return resp.status, json.loads(text) if text else None
    except urllib.error.HTTPError as e:
        text = e.read().decode()
        return e.code, json.loads(text) if text else None


class Env:
    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.procs: dict[str, subprocess.Popen] = {}
        self.supervised: dict[str, tuple[str, tuple[str, ...], dict]] = {}
        self.restarts: dict[str, int] = {}

    # --- processes ------------------------------------------------------------------------------

    def start(
        self, name: str, module: str, *args: str, supervise: bool = False, env: dict | None = None
    ) -> None:
        log = open(self.run_dir / f"{name}.log", "a", encoding="utf8")  # noqa: SIM115
        self.procs[name] = subprocess.Popen(
            [sys.executable, "-u", "-m", module, *args],
            cwd=ROOT,
            env={**ENV, **(env or {})},
            stdout=log,
            stderr=log,
        )
        if supervise:
            self.supervised[name] = (module, args, env or {})

    def kill(self, name: str) -> None:
        """Abrupt: TerminateProcess on Windows; no clean shutdown runs."""
        proc = self.procs.pop(name, None)
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=30)

    def alive(self, name: str) -> bool:
        return name in self.procs and self.procs[name].poll() is None

    def supervise(self) -> None:
        """Restart a supervised process that died, as an orchestrator would, and count it."""
        for name, (module, args, env) in list(self.supervised.items()):
            proc = self.procs.get(name)
            if proc is not None and proc.poll() is not None:
                self.restarts[name] = self.restarts.get(name, 0) + 1
                time.sleep(1)
                self.start(name, module, *args, supervise=True, env=env)

    def unsupervise(self, name: str) -> None:
        self.supervised.pop(name, None)

    def wait_proc(self, name: str, timeout: float) -> int:
        proc = self.procs[name]
        deadline = time.monotonic() + timeout
        while proc.poll() is None:
            if time.monotonic() > deadline:
                raise TimeoutError(f"{name} still running")
            self.supervise()
            time.sleep(1)
        self.procs.pop(name)
        return proc.returncode

    def log(self, name: str) -> str:
        return (self.run_dir / f"{name}.log").read_text(encoding="utf8", errors="replace")

    def stop_all(self) -> None:
        self.supervised.clear()
        for name in list(self.procs):
            self.kill(name)

    def start_apply(self, apply_module: str = "migration.apply", env: dict | None = None) -> None:
        self.start("apply-dublin", apply_module, "dublin", supervise=True, env=env)
        self.start("apply-helsinki", apply_module, "helsinki", supervise=True, env=env)

    # --- state ----------------------------------------------------------------------------------

    def reset(self, apply_module: str = "migration.apply", during_snapshot=None) -> None:
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
        if during_snapshot is not None:
            during_snapshot(self)
        self.wait_snapshot()

    def _gateway_to_legacy(self) -> None:
        gw = "http://localhost:8000"
        http("POST", gw + "/admin/fence")
        http(
            "POST",
            gw + "/admin/route",
            {"dublin": "http://api-a:8000", "helsinki": "http://api-b:8000"},
        )
        http("POST", gw + "/admin/release")
        for port in (8001, 8002):
            http("POST", f"http://localhost:{port}/admin/writes?open=true")

    def connect(self, method: str, path: str, body: dict | None = None):
        return http(method, "http://localhost:8083" + path, body)

    def wait_connectors(self, names=("dublin", "helsinki"), timeout: float = 120) -> None:
        def running():
            for name in names:
                status, s = self.connect("GET", f"/connectors/{name}/status")
                if (
                    status != 200
                    or s["connector"]["state"] != "RUNNING"
                    or any(t["state"] != "RUNNING" for t in s["tasks"])
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
                    "SELECT (SELECT count(*) FROM fleet.station_status) + "
                    "(SELECT count(*) FROM fleet.trip) + "
                    "(SELECT count(*) FROM migration.quarantine)"
                ).fetchone()[0]
            return got >= readings + rentals + trips and self.lag() == 0

        self.wait_until(done, timeout, "snapshot applied")

    def wait_drained(self, timeout: float = 300) -> None:
        self.wait_until(lambda: self.lag() == 0, timeout, "apply drained")
        time.sleep(3)
        self.wait_until(lambda: self.lag() == 0, timeout, "apply drained")

    def wait_until(self, predicate, timeout: float, what: str, every: float = 2) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.supervise()
            try:
                if predicate():
                    return
            except Exception:  # noqa: BLE001  a target restarting does not end the wait
                pass
            time.sleep(every)
        raise TimeoutError(f"timed out waiting for {what}")

    # --- load and results -----------------------------------------------------------------------

    def start_generator(self, seconds: float, rate: float, seed: int, run_id: str) -> None:
        self.start(
            "generator",
            "generator.run",
            "--rate",
            str(rate),
            "--seconds",
            str(seconds),
            "--seed",
            str(seed),
            "--run-id",
            run_id,
        )

    def generator_summary(self, run_id: str) -> dict:
        return json.loads((ROOT / "data" / "runs" / run_id / "summary.json").read_text())

    def generator_requests(self, run_id: str) -> list[dict]:
        path = ROOT / "data" / "runs" / run_id / "requests.jsonl"
        return [json.loads(line) for line in path.read_text(encoding="utf8").splitlines()]

    def generate(self, seconds: float, rate: float, seed: int, run_id: str) -> dict:
        self.start_generator(seconds, rate, seed, run_id)
        self.wait_proc("generator", seconds + 600)
        return self.generator_summary(run_id)

    def verify(self) -> dict:
        for p in (ROOT / "data" / "verify").glob("verify.duckdb*"):
            p.unlink()
        py("verifier.run", check=False, timeout=1800)
        return json.loads((ROOT / "data" / "verify" / "report.json").read_text(encoding="utf8"))
