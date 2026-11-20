"""The chaos suite: python -m chaos.run [scenario ...]

Each scenario runs in a fresh environment (`Env.reset`), declares before it runs what the
verifier must report, injects its fault, and is judged against that declaration and nothing else.
A scenario passes only if the observation equals the declaration: a check that fires when it was
not supposed to fails the scenario as surely as one that stays silent when it should fire.

Results go to data/chaos/<scenario>/result.json and a summary to data/chaos/summary.json.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime

from chaos.env import ROOT, Env, helsinki
from chaos.planted import PLANT_EVERY

OUT = ROOT / "data" / "chaos"


def clean_tables() -> dict:
    return {
        t: {"missing": 0, "unexpected": 0, "different": 0, "columns": {}}
        for t in ("station", "station_status", "trip")
    }


def observed(report: dict) -> dict:
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


# --- scenarios ----------------------------------------------------------------------------------


def s10_planted_bug(env: Env) -> tuple[dict, dict]:
    """A unit bug in the transform: the verifier must name those rows and that column only."""
    b = helsinki()
    with b.cursor() as cur:
        # The planted rows that show: distance known, not negative (that becomes null), not zero.
        cur.execute(
            f"SELECT count(*) FROM matka WHERE id % {PLANT_EVERY} = 0 AND covered_distance_m > 0"
        )
        n = cur.fetchone()[0]
    b.close()
    declared = {
        "tables": clean_tables(),
        "quarantine": {"only_expected": 0, "only_target": 0},
        "rows_equal": True,
    }
    declared["tables"]["trip"] = {
        "missing": 0,
        "unexpected": 0,
        "different": n,
        "columns": {"distance_m": n},
    }
    return declared, observed(env.verify())


SCENARIOS = {
    "10-planted-bug": (s10_planted_bug, {"apply_module": "chaos.planted"}),
}


def run(name: str) -> dict:
    fn, options = SCENARIOS[name]
    env = Env(OUT / name)
    started = time.monotonic()
    result: dict = {"scenario": name, "started_at": datetime.now(UTC).isoformat()}
    try:
        env.reset(options.get("apply_module", "migration.apply"))
        result["reset_s"] = round(time.monotonic() - started, 1)
        declared, seen = fn(env)
        result.update(declared=declared, observed=seen, passed=declared == seen)
    except Exception as e:  # noqa: BLE001
        result.update(passed=False, error=f"{type(e).__name__}: {e}")
    finally:
        env.stop_all()
    result["duration_s"] = round(time.monotonic() - started, 1)
    (env.run_dir / "result.json").write_text(
        json.dumps(result, indent=2, default=str) + "\n", encoding="utf8"
    )
    print(
        f"{name}: {'PASS' if result['passed'] else 'FAIL'} ({result['duration_s']} s)", flush=True
    )
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
            **({"error": r["error"]} if "error" in r else {}),
        }
        for r in results
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
