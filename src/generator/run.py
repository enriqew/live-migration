"""Run the generator: python -m generator.run --rate 100 --seconds 60 [--seed 1]

Writes one JSON line per operation to data/runs/<run_id>/requests.jsonl and prints a summary.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from generator.ops import Op, OpStream, World

ROOT = Path(__file__).resolve().parents[2]
RETRYABLE_STATUS = {502, 503, 504}
ACCEPTED, REFUSED = "accepted", "refused"  # refused: the business said no (404, 409)


@dataclass
class Outcome:
    op: Op
    due: float  # monotonic seconds
    finished: float
    status: int | None
    attempts: int
    result: str  # 'accepted' | 'refused' | 'failed'
    error: str | None = None

    def record(self, t0_wall: float, t0_mono: float) -> dict:
        def wall(mono: float) -> str:
            return datetime.fromtimestamp(t0_wall + (mono - t0_mono), UTC).isoformat()

        return {
            "seq": self.op.seq,
            "system": self.op.system,
            "kind": self.op.kind,
            "method": self.op.method,
            "path": self.op.path,
            "key": self.op.key,
            "due_at": wall(self.due),
            "finished_at": wall(self.finished),
            "latency_ms": round((self.finished - self.due) * 1000, 3),
            "status": self.status,
            "attempts": self.attempts,
            "result": self.result,
            "error": self.error,
        }


async def send(
    client: httpx.AsyncClient, op: Op, due: float, deadline_s: float
) -> tuple[Outcome, httpx.Response | None]:
    attempts, backoff = 0, 0.05
    error = None
    while True:
        attempts += 1
        try:
            resp = await client.request(
                op.method, op.path, json=op.body, headers={"Idempotency-Key": op.key}
            )
            if resp.status_code not in RETRYABLE_STATUS:
                result = (
                    ACCEPTED
                    if resp.is_success
                    else REFUSED
                    if resp.status_code in (404, 409)
                    else "failed"
                )
                detail = None if resp.is_success else resp.text[:200]
                outcome = Outcome(
                    op, due, time.monotonic(), resp.status_code, attempts, result, detail
                )
                return outcome, resp
            error = f"HTTP {resp.status_code}"
        except httpx.TransportError as e:  # includes timeouts and refused connections
            error = f"{type(e).__name__}: {e}"[:200]
        if time.monotonic() - due + backoff > deadline_s:
            return Outcome(op, due, time.monotonic(), None, attempts, "failed", error), None
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 1.0)


async def discover(dublin: httpx.AsyncClient, helsinki: httpx.AsyncClient) -> World:
    d = (await dublin.get("/stations")).raise_for_status().json()["stations"]
    h = (await helsinki.get("/asemat")).raise_for_status().json()["asemat"]
    m = (await helsinki.get("/matkat/max-id")).raise_for_status().json()["max_id"]
    return World(dublin_stations=d, helsinki_stations=h, helsinki_max_trip_id=m)


async def run(args) -> dict:
    run_id = args.run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir = ROOT / "data" / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    limits = httpx.Limits(max_connections=args.max_in_flight)
    timeout = httpx.Timeout(args.request_timeout)
    async with (
        httpx.AsyncClient(base_url=args.dublin, limits=limits, timeout=timeout) as dublin,
        httpx.AsyncClient(base_url=args.helsinki, limits=limits, timeout=timeout) as helsinki,
    ):
        world = await discover(dublin, helsinki)
        stream = OpStream(args.seed, run_id, world, share_dublin=args.share_dublin)
        clients = {"dublin": dublin, "helsinki": helsinki}
        outcomes: list[Outcome] = []
        pending: set[asyncio.Task] = set()

        async def one(op: Op, due: float):
            outcome, resp = await send(clients[op.system], op, due, args.deadline)
            if outcome.result == ACCEPTED and op.on_success and resp is not None:
                data = resp.json()
                if op.on_success == "open_rental":
                    world.open_rentals.append(data["rental_id"])
                elif op.on_success == "own_trip":
                    world.own_trips.append(data["id"])
            elif outcome.result != ACCEPTED and op.rental is not None:
                world.open_rentals.append(op.rental)
            outcomes.append(outcome)

        t0_wall, t0 = time.time(), time.monotonic()
        total = int(args.rate * args.seconds)
        for i in range(total):
            due = t0 + i / args.rate
            delay = due - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            task = asyncio.create_task(one(stream.next(), due))
            pending.add(task)
            task.add_done_callback(pending.discard)
        await asyncio.gather(*pending)

    outcomes.sort(key=lambda o: o.op.seq)
    with (out_dir / "requests.jsonl").open("w", encoding="utf8") as fh:
        for o in outcomes:
            fh.write(json.dumps(o.record(t0_wall, t0)) + "\n")
    summary = summarize(outcomes, args, run_id)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf8")
    return summary


def percentile(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    i = min(len(sorted_values) - 1, max(0, round(q * (len(sorted_values) - 1))))
    return sorted_values[i]


def summarize(outcomes: list[Outcome], args, run_id: str) -> dict:
    by_result: dict[str, int] = {}
    for o in outcomes:
        by_result[o.result] = by_result.get(o.result, 0) + 1
    latencies = sorted((o.finished - o.due) * 1000 for o in outcomes)
    return {
        "run_id": run_id,
        "seed": args.seed,
        "rate_per_s": args.rate,
        "seconds": args.seconds,
        "operations": len(outcomes),
        "by_result": by_result,
        "retried": sum(o.attempts > 1 for o in outcomes),
        "latency_ms_from_due": {
            "p50": percentile(latencies, 0.5),
            "p99": percentile(latencies, 0.99),
            "max": latencies[-1] if latencies else None,
        },
    }


def main() -> None:
    p = argparse.ArgumentParser(prog="generator.run")
    p.add_argument("--rate", type=float, default=50.0, help="operations per second, both systems")
    p.add_argument("--seconds", type=float, default=60.0)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--run-id")
    p.add_argument("--share-dublin", type=float, default=0.5)
    p.add_argument("--dublin", default="http://127.0.0.1:8001")
    p.add_argument("--helsinki", default="http://127.0.0.1:8002")
    p.add_argument("--max-in-flight", type=int, default=200)
    p.add_argument("--request-timeout", type=float, default=5.0)
    p.add_argument("--deadline", type=float, default=30.0, help="give up on an op after this")
    summary = asyncio.run(run(p.parse_args()))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
