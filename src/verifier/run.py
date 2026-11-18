"""python -m verifier.run [--keep]

The frozen check: with the generator stopped and the apply services caught up, copy the legacy
systems and the target into a local DuckDB file, rebuild the expected target from the legacy
copies with `verifier.spec`, and compare.

Three levels, cheapest first, as the plan specifies:
1. row counts per bucket (the first two hex digits of the id: 256 buckets per table);
2. a hash per bucket: SHA-256 over the row hashes in id order, each row hash taken over the
   canonical form of the row (MAPPING_SPEC section 7);
3. row by row, only inside buckets whose hash differs: which rows are missing, unexpected or
   different, and in which columns.

The quarantine is compared as a set: the rows each implementation refused, and under which rule.
Writes `data/verify/report.json` and exits non-zero on any difference.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import duckdb

from verifier import spec

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "verify"
LEGACY_A = "host=localhost port=5433 dbname=dublin user=dublin password=dublin"
LEGACY_B = "host=127.0.0.1 port=3307 user=helsinki password=helsinki database=helsinki"
TARGET = "host=localhost port=5434 dbname=fleet user=fleet password=fleet"

COPIES = {
    "la_station": "a.public.station",
    "la_station_reading": "a.public.station_reading",
    "la_rental": "a.public.rental",
    "lb_asema": "b.helsinki.asema",
    "lb_matka": "b.helsinki.matka",
    "t_station": "t.fleet.station",
    "t_status": "t.fleet.station_status",
    "t_trip": "t.fleet.trip",
    "t_quarantine": "t.migration.quarantine",
}

TABLES = {
    "station": (
        "station_id",
        [
            "station_id",
            "system_code",
            "source_ref",
            "name",
            "name_sv",
            "name_en",
            "address",
            "city",
            "capacity",
            "latitude",
            "longitude",
            "kind",
        ],
    ),
    "station_status": (
        "status_id",
        [
            "status_id",
            "station_id",
            "reported_at",
            "polled_at",
            "bikes_available",
            "docks_available",
            "is_open",
            "is_renting",
            "is_returning",
        ],
    ),
    "trip": (
        "trip_id",
        [
            "trip_id",
            "system_code",
            "departure_station_id",
            "return_station_id",
            "departed_at",
            "returned_at",
            "duration_s",
            "distance_m",
            "flags",
        ],
    ),
}
EXPECTED = {
    "station": "SELECT * FROM exp_station_dublin UNION ALL SELECT * FROM exp_station_helsinki",
    "station_status": "SELECT * FROM exp_status",
    "trip": "SELECT * EXCLUDE (dep_ref, ret_ref) FROM exp_trip_helsinki "
    "UNION ALL SELECT * FROM exp_trip_dublin",
}
ACTUAL = {"station": "t_station", "station_status": "t_status", "trip": "t_trip"}


def canonical(col: str, dtype: str) -> str:
    """The canonical text of one column (MAPPING_SPEC section 7), null as \\N."""
    if "TIMESTAMP" in dtype:
        expr = f"strftime({col}, '%Y-%m-%dT%H:%M:%S.%fZ')"
    elif dtype == "BOOLEAN":
        expr = f"CASE WHEN {col} THEN 't' ELSE 'f' END"
    elif dtype.endswith("[]"):
        expr = f"'{{' || array_to_string(list_sort({col}), ',') || '}}'"
    else:
        expr = f"CAST({col} AS VARCHAR)"
    return f"coalesce({expr}, '\\N')"


def hashed(con, name: str, source: str, pk: str, cols: list[str]) -> None:
    types = dict(con.execute(f"SELECT column_name, data_type FROM (DESCRIBE {source})").fetchall())
    parts = " || chr(31) || ".join(canonical(c, types[c]) for c in cols)
    con.execute(
        f"CREATE OR REPLACE TABLE {name} AS SELECT {pk} AS id, "
        f"substr(CAST({pk} AS VARCHAR), 1, 2) AS bucket, sha256({parts}) AS h, "
        f"{', '.join(cols)} FROM ({source})"
    )


def compare(con, table: str) -> dict:
    pk, cols = TABLES[table]
    hashed(
        con,
        f"e_{table}",
        f"SELECT * FROM ({EXPECTED[table]}) WHERE quarantine_rule IS NULL",
        pk,
        cols,
    )
    hashed(con, f"a_{table}", f"SELECT * FROM {ACTUAL[table]}", pk, cols)
    buckets = con.execute(f"""
        WITH e AS (SELECT bucket, count(*) n, sha256(string_agg(h, '' ORDER BY id)) bh
                   FROM e_{table} GROUP BY bucket),
             a AS (SELECT bucket, count(*) n, sha256(string_agg(h, '' ORDER BY id)) bh
                   FROM a_{table} GROUP BY bucket)
        SELECT coalesce(e.bucket, a.bucket), coalesce(e.n, 0), coalesce(a.n, 0),
               e.bh IS NOT DISTINCT FROM a.bh
        FROM e FULL JOIN a USING (bucket)""").fetchall()
    bad = [b for b, _, _, same in buckets if not same]
    result = {
        "expected_rows": sum(n for _, n, _, _ in buckets),
        "target_rows": sum(n for _, _, n, _ in buckets),
        "buckets": len(buckets),
        "buckets_count_differs": sum(1 for _, ne, na, _ in buckets if ne != na),
        "buckets_hash_differs": len(bad),
    }
    if not bad:
        result.update(missing=0, unexpected=0, different=0, columns={})
        return result
    rows = con.execute(
        f"""
        SELECT e.id AS eid, a.id AS aid,
               {", ".join(f"e.{c} IS NOT DISTINCT FROM a.{c} AS same_{c}" for c in cols)}
        FROM (SELECT * FROM e_{table} WHERE bucket IN ?) e
        FULL JOIN (SELECT * FROM a_{table} WHERE bucket IN ?) a ON e.id = a.id
        WHERE e.h IS DISTINCT FROM a.h""",
        [bad, bad],
    ).fetchall()
    missing = sum(1 for r in rows if r[1] is None)
    unexpected = sum(1 for r in rows if r[0] is None)
    columns: dict[str, int] = {}
    samples = []
    for r in rows:
        if r[0] is None or r[1] is None:
            continue
        for c, same in zip(cols, r[2:], strict=True):
            if not same:
                columns[c] = columns.get(c, 0) + 1
        if len(samples) < 5:
            samples.append(str(r[0]))
    result.update(
        missing=missing,
        unexpected=unexpected,
        different=len(rows) - missing - unexpected,
        columns=columns,
        sample_different_ids=samples,
    )
    return result


def compare_quarantine(con) -> dict:
    con.execute("""
        CREATE OR REPLACE TABLE e_quarantine AS
        SELECT 'dublin' AS source, 'station' AS legacy_table, legacy_key, quarantine_rule AS rule
            FROM exp_station_dublin WHERE quarantine_rule IS NOT NULL
        UNION ALL SELECT 'dublin', 'station_reading', legacy_key, quarantine_rule
            FROM exp_status WHERE quarantine_rule IS NOT NULL
        UNION ALL SELECT 'dublin', 'rental', legacy_key, quarantine_rule
            FROM exp_trip_dublin WHERE quarantine_rule IS NOT NULL
        UNION ALL SELECT 'helsinki', 'asema', legacy_key, quarantine_rule
            FROM exp_station_helsinki WHERE quarantine_rule IS NOT NULL
        UNION ALL SELECT 'helsinki', 'matka', legacy_key, quarantine_rule
            FROM exp_trip_helsinki WHERE quarantine_rule IS NOT NULL""")
    only_e = con.execute(
        "SELECT count(*) FROM (SELECT source, legacy_table, legacy_key, rule "
        "FROM e_quarantine EXCEPT SELECT source, legacy_table, legacy_key, rule "
        "FROM t_quarantine)"
    ).fetchone()[0]
    only_a = con.execute(
        "SELECT count(*) FROM (SELECT source, legacy_table, legacy_key, rule "
        "FROM t_quarantine EXCEPT SELECT source, legacy_table, legacy_key, rule "
        "FROM e_quarantine)"
    ).fetchone()[0]
    by_rule = dict(con.execute("SELECT rule, count(*) FROM e_quarantine GROUP BY 1").fetchall())
    return {
        "expected": sum(by_rule.values()),
        "by_rule": by_rule,
        "only_expected": only_e,
        "only_target": only_a,
    }


def main() -> None:
    p = argparse.ArgumentParser(prog="verifier.run")
    p.add_argument("--keep", action="store_true", help="reuse the copies already in the file")
    args = p.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(OUT / "verify.duckdb"))
    con.execute("SET TimeZone = 'UTC'")
    for ext in ("postgres", "mysql", "icu"):
        con.execute(f"INSTALL {ext}; LOAD {ext};")
    timings = {}
    t0 = time.monotonic()
    if not args.keep:
        con.execute(f"ATTACH '{LEGACY_A}' AS a (TYPE postgres, READ_ONLY)")
        con.execute(f"ATTACH '{LEGACY_B}' AS b (TYPE mysql, READ_ONLY)")
        con.execute(f"ATTACH '{TARGET}' AS t (TYPE postgres, READ_ONLY)")
        for local, remote in COPIES.items():
            con.execute(f"CREATE OR REPLACE TABLE {local} AS SELECT * FROM {remote}")
            print(
                f"copied {local}: {con.execute(f'SELECT count(*) FROM {local}').fetchone()[0]:,}",
                flush=True,
            )
    timings["copy_s"] = round(time.monotonic() - t0, 1)
    t1 = time.monotonic()
    con.execute(spec.MACROS)
    con.execute(spec.TZ_HOURS)
    con.execute(spec.DUBLIN)
    con.execute(spec.HELSINKI)
    timings["spec_s"] = round(time.monotonic() - t1, 1)
    t2 = time.monotonic()
    report = {"tables": {t: compare(con, t) for t in TABLES}, "quarantine": compare_quarantine(con)}
    timings["compare_s"] = round(time.monotonic() - t2, 1)
    report["timings"] = timings
    clean = (
        all(
            r["buckets_hash_differs"] == 0 and r["expected_rows"] == r["target_rows"]
            for r in report["tables"].values()
        )
        and report["quarantine"]["only_expected"] == 0
        and report["quarantine"]["only_target"] == 0
    )
    report["clean"] = clean
    (OUT / "report.json").write_text(
        json.dumps(report, indent=2, default=str) + "\n", encoding="utf8"
    )
    print(json.dumps(report, indent=2, default=str))
    sys.exit(0 if clean else 1)


if __name__ == "__main__":
    main()
