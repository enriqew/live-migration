"""The chaos suite: python -m chaos.run [scenario ...]

Each scenario starts from a fresh environment (`Env.reset`), declares what must happen, injects
its fault, and is judged against that declaration and nothing else. A scenario passes only if the
observation equals the declaration: a check that fires when it was not supposed to fails the
scenario as surely as one that stays silent when it should fire.

Results go to data/chaos/<scenario>/result.json and a summary to data/chaos/summary.json.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime

from chaos.env import ROOT, Env
from chaos.scenarios import SCENARIOS

OUT = ROOT / "data" / "chaos"


def run(name: str) -> dict:
    env = Env(OUT / name)
    for old in env.run_dir.glob("*.log"):
        old.unlink()
    started = time.monotonic()
    result: dict = {"scenario": name, "started_at": datetime.now(UTC).isoformat()}
    try:
        declared, seen = SCENARIOS[name](env)
        mismatched = sorted(k for k in declared if declared[k] != seen.get(k))
        result.update(
            declared=declared,
            observed=seen,
            mismatched=mismatched,
            passed=not mismatched and set(declared) == set(seen),
        )
    except Exception as e:  # noqa: BLE001
        result.update(passed=False, error=f"{type(e).__name__}: {e}")
    finally:
        env.stop_all()
    result["restarts"] = env.restarts
    result["duration_s"] = round(time.monotonic() - started, 1)
    (env.run_dir / "result.json").write_text(
        json.dumps(result, indent=2, default=str) + "\n", encoding="utf8"
    )
    verdict = (
        "PASS" if result["passed"] else f"FAIL {result.get('mismatched') or result.get('error')}"
    )
    print(f"{name}: {verdict} ({result['duration_s']} s)", flush=True)
    return result


def main() -> None:
    p = argparse.ArgumentParser(prog="chaos.run")
    p.add_argument("scenarios", nargs="*", default=list(SCENARIOS))
    names = p.parse_args().scenarios
    OUT.mkdir(parents=True, exist_ok=True)
    results = [run(n) for n in names]
    summary = {
        r["scenario"]: {
            "passed": r["passed"],
            "duration_s": r["duration_s"],
            **({"mismatched": r["mismatched"]} if r.get("mismatched") else {}),
            **({"error": r["error"]} if "error" in r else {}),
        }
        for r in results
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
