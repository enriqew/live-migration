"""python -m export [--out export]

The static files the portfolio page reads, built from the evidence of the full-size runs and
nothing else. It refuses to write anything when a run behind them did not come out clean, so the
page can only ever show runs that passed:

- the run from a clean start (load and snapshot under writes) must be ok and its verifier clean;
- the cycle (cutover and rollback under load) must be ok, its verifier clean, and no write failed;
- the continuous pass must be clean, and the markers in the target must be exactly its chunks;
- the chaos suite must have every scenario, and every one passed.

The runs are named here, not discovered: a later run cannot replace the published one without a
commit that says so. Output is sorted and carries no export time, so the same evidence always
gives the same bytes.

| file | what the page shows with it |
|---|---|
| manifest.json | the headline figures and the run each comes from |
| pause.json | every request around the cutover's and the rollback's fence, and the steps inside |
| latency.json | request latency per second through the cycle, and per 10 s through the continuous pass |
| snapshot.json | rows applied over time during the snapshot, while the generator wrote |
| chunks.json | the continuous pass, chunk by chunk |
| chaos.json | each scenario, what it declared and what it saw |
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[2]
RUNS = ROOT / "data" / "runs"
TARGET = "host=localhost port=5434 dbname=fleet user=fleet password=fleet"

FULL_RUN = "full-20261120T190007Z"  # from a clean start: load, snapshot under writes, cycle
CYCLE_RUN = "cycle-20261121T072905Z"  # the cycle again on that state, after the fixes
CYCLE_REQUESTS = "full-20261121T072905Z-cycle"
CONTINUOUS_REPORT = "continuous-20261122T173528Z.json"
CONTINUOUS_REQUESTS = "continuous-20261122T165611Z-load"
CHAOS_SCENARIOS = 9


class Refused(RuntimeError):
    pass


def check(ok: bool, why: str) -> None:
    if not ok:
        raise Refused(why)


def read(path: Path):
    return json.loads(path.read_text(encoding="utf8"))


def requests(run_id: str) -> list[dict]:
    with (RUNS / run_id / "requests.jsonl").open(encoding="utf8") as f:
        return [json.loads(line) for line in f]


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def ms(x: float) -> float:
    return round(x, 1)


# --- the pauses --------------------------------------------------------------------------------


def pause(kind: str, record: dict, reqs: list[dict], margin_s: float = 3.0) -> dict:
    """Every request due from a few seconds before the fence to a few after the release."""
    start = ts(record["started_at"])
    steps = {s["step"]: s for s in record["steps"]}
    fence = start + timedelta(milliseconds=steps["fence"]["at_ms"])
    release = start + timedelta(milliseconds=steps["released"]["at_ms"])
    lo, hi = fence - timedelta(seconds=margin_s), release + timedelta(seconds=margin_s)
    points = []
    for r in reqs:
        due = ts(r["due_at"])
        if lo <= due <= hi:
            points.append(
                {
                    "t_ms": ms((due - fence).total_seconds() * 1000),
                    "latency_ms": ms(r["latency_ms"]),
                    "system": r["system"],
                    "result": r["result"],
                }
            )
    points.sort(key=lambda p: p["t_ms"])
    fence_ms = steps["fence"]["at_ms"]
    return {
        "kind": kind,
        "fenced_ms": steps["released"]["fenced_ms"],
        "held_max": steps["released"]["held_max"],
        # Steps relative to the fence, the moment the gateway starts holding requests.
        "steps": [
            {"step": s["step"], "t_ms": ms(s["at_ms"] - fence_ms)}
            for s in record["steps"]
            if s["at_ms"] >= fence_ms
        ],
        "window_ms": [-margin_s * 1000, ms((release - fence).total_seconds() * 1000 + margin_s * 1000)],
        "requests": points,
        "failed": sum(1 for p in points if p["result"] == "failed"),
    }


# --- latency over a run ------------------------------------------------------------------------


def series(reqs: list[dict], bucket_s: int) -> list[dict]:
    t0 = min(ts(r["due_at"]) for r in reqs)
    buckets: dict[int, list[float]] = {}
    for r in reqs:
        k = int((ts(r["due_at"]) - t0).total_seconds() // bucket_s)
        buckets.setdefault(k, []).append(r["latency_ms"])
    return [
        {
            "t_s": k * bucket_s,
            "n": len(v),
            "p50_ms": ms(pct(v, 0.5)),
            "p99_ms": ms(pct(v, 0.99)),
            "max_ms": ms(max(v)),
        }
        for k, v in sorted(buckets.items())
    ]


def overall(reqs: list[dict]) -> dict:
    lat = [r["latency_ms"] for r in reqs]
    return {
        "operations": len(reqs),
        "failed": sum(1 for r in reqs if r["result"] == "failed"),
        "retried": sum(1 for r in reqs if r["attempts"] > 1),
        "p50_ms": ms(pct(lat, 0.5)),
        "p99_ms": ms(pct(lat, 0.99)),
        "max_ms": ms(max(lat)),
    }


# --- the snapshot ------------------------------------------------------------------------------

APPLIED = re.compile(r"^(\d\d:\d\d:\d\d) applied ([\d,]+) ")


def snapshot(run_dir: Path, day: str, until: datetime) -> list[dict]:
    """Rows applied per source over time, read from the apply services' own progress lines,
    up to the cutover: the load and the snapshot, with the generator writing throughout."""
    points: dict[str, dict[str, int]] = {}
    for source in ("dublin", "helsinki"):
        for line in (run_dir / f"apply-{source}.log").read_text(encoding="utf8").splitlines():
            m = APPLIED.match(line)
            if m:
                points.setdefault(m.group(1), {})[source] = int(m.group(2).replace(",", ""))
    out, last = [], {"dublin": 0, "helsinki": 0}
    t0 = None
    for hhmmss in sorted(points):
        at = datetime.fromisoformat(f"{day}T{hhmmss}+00:00")
        if at >= until:
            break
        t0 = t0 or at
        last.update(points[hhmmss])
        out.append({"t_s": int((at - t0).total_seconds()), **last})
    return out


# --- the continuous pass -----------------------------------------------------------------------


def chunks(report: dict) -> dict:
    with psycopg.connect(TARGET) as t:
        rows = t.execute(
            "SELECT legacy_table, lo, hi, applied_at FROM migration.verify_marker_applied "
            "WHERE applied_at BETWEEN %s AND %s ORDER BY legacy_table, lo",
            (ts(report["started_at"]), ts(report["finished_at"])),
        ).fetchall()
    t0 = ts(report["started_at"])
    by: dict[str, list] = {}
    for table, lo, hi, applied in rows:
        by.setdefault(table, []).append([lo, hi, round((applied - t0).total_seconds(), 1)])
    for table, summary in report["tables"].items():
        check(
            len(by.get(table, [])) == summary["chunks"],
            f"{table}: {len(by.get(table, []))} markers in the target, the report has "
            f"{summary['chunks']} chunks",
        )
    return {
        "tables": {
            t: {**report["tables"][t], "chunks_list": by[t]} for t in sorted(report["tables"])
        },
        "elapsed_s": report["elapsed_s"],
        "lock_ms": report["lock_ms"],
        "findings": len(report["findings"]),
    }


# --- the chaos suite ---------------------------------------------------------------------------


def scenario_titles() -> dict[str, str]:
    """The first sentence of each scenario's docstring, read from the source, not imported."""
    tree = ast.parse((ROOT / "src" / "chaos" / "scenarios.py").read_text(encoding="utf8"))
    out = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and re.match(r"s\d\d_", node.name):
            doc = " ".join((ast.get_docstring(node) or "").split())
            out[node.name[1:3]] = re.split(r"(?<=\.)\s", doc, maxsplit=1)[0]
    return out


def chaos() -> list[dict]:
    titles = scenario_titles()
    out = []
    for path in sorted((ROOT / "data" / "chaos").glob("*/result.json")):
        r = read(path)
        n = r["scenario"][:2]

        def facts(d: dict) -> dict:
            keep = {k: v for k, v in d.items() if k not in ("tables", "quarantine")}
            diffs = {
                t: v for t, v in d.get("tables", {}).items() if v.get("different") or v.get("missing")
            }
            if diffs:
                keep["tables"] = diffs
            return keep

        out.append(
            {
                "id": r["scenario"],
                "title": titles[n],
                "passed": r["passed"],
                "duration_s": r["duration_s"],
                "declared": facts(r["declared"]),
                "observed": facts(r["observed"]),
            }
        )
    return out


# --- all together ------------------------------------------------------------------------------


def build() -> dict[str, object]:
    full = read(RUNS / FULL_RUN / "run.json")
    cycle = read(RUNS / CYCLE_RUN / "run.json")
    report = read(ROOT / "data" / "verify" / CONTINUOUS_REPORT)
    check(full["ok"] and full["verify"]["clean"], f"{FULL_RUN} is not ok")
    check(cycle["ok"] and cycle["verify"]["clean"], f"{CYCLE_RUN} is not ok")
    check(cycle["cutover"]["ok"] and cycle["rollback"]["ok"], "the cycle's cutover or rollback failed")
    check(report["clean"] and not report["findings"], "the continuous pass has findings")

    cycle_reqs = requests(CYCLE_REQUESTS)
    load_reqs = requests(CONTINUOUS_REQUESTS)
    cyc, load = overall(cycle_reqs), overall(load_reqs)
    check(cyc["failed"] == 0, f"{cyc['failed']} writes failed in the cycle")
    check(load["failed"] == 0, f"{load['failed']} writes failed during the continuous pass")

    suite = chaos()
    check(len(suite) == CHAOS_SCENARIOS, f"{len(suite)} chaos scenarios, expected {CHAOS_SCENARIOS}")
    check(all(s["passed"] for s in suite), "a chaos scenario failed")

    cut = pause("cutover", cycle["cutover"], cycle_reqs)
    back = pause("rollback", cycle["rollback"], cycle_reqs)
    check(cut["failed"] == 0 and back["failed"] == 0, "a write failed during a fence")

    tables = cycle["verify"]["tables"]
    day = f"{FULL_RUN[5:9]}-{FULL_RUN[9:11]}-{FULL_RUN[11:13]}"
    snap = snapshot(RUNS / FULL_RUN, day, ts(full["cutover"]["started_at"]))
    manifest = {
        "runs": {
            "full": FULL_RUN,
            "cycle": CYCLE_RUN,
            "continuous": CONTINUOUS_REPORT.removesuffix(".json"),
        },
        "rows": {t: v["target_rows"] for t, v in sorted(tables.items())},
        "rows_total": sum(v["target_rows"] for v in tables.values()),
        "frozen_verify": {"clean": True, "rows": sum(v["target_rows"] for v in tables.values())},
        "cutover": {"fenced_ms": cut["fenced_ms"], "held_max": cut["held_max"]},
        "rollback": {"fenced_ms": back["fenced_ms"], "held_max": back["held_max"]},
        "cycle": cyc,
        "continuous": {
            "rows": sum(t["rows"] for t in report["tables"].values()),
            "chunks": sum(t["chunks"] for t in report["tables"].values()),
            "elapsed_s": report["elapsed_s"],
            "lock_ms": report["lock_ms"],
            "writes": load,
        },
        "chaos": {"scenarios": len(suite), "passed": sum(s["passed"] for s in suite)},
    }
    return {
        "manifest.json": manifest,
        "pause.json": {"cutover": cut, "rollback": back},
        "latency.json": {
            "cycle": {"bucket_s": 1, "points": series(cycle_reqs, 1), **cyc},
            "continuous": {"bucket_s": 10, "points": series(load_reqs, 10), **load},
        },
        "snapshot.json": {"run": FULL_RUN, "points": snap},
        "chunks.json": chunks(report),
        "chaos.json": suite,
    }


def main() -> None:
    p = argparse.ArgumentParser(prog="export")
    p.add_argument("--out", default=str(ROOT / "export"))
    out = Path(p.parse_args().out)
    try:
        files = build()
    except Refused as e:
        print(f"export refused, nothing written: {e}", file=sys.stderr)
        sys.exit(1)
    out.mkdir(parents=True, exist_ok=True)
    for name, data in files.items():
        text = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str) + "\n"
        (out / name).write_text(text, encoding="utf8", newline="\n")
        print(f"{name}: {len(text) / 1024:,.1f} KB")


if __name__ == "__main__":
    main()
