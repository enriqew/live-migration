"""python -m verifier.continuous [--tables matka ...] [--chunk 10000] [--max-chunks N]

The continuous check: the frozen check's spec and canonical form, chunk by chunk, while the
legacy systems keep taking writes and the apply services keep running.

A chunk is a range of one legacy table's keys (the whole register, for stations). For each one:

1. **Source side.** In one transaction on the legacy system: read the chunk with FOR SHARE, so no
   write to those rows can commit until this transaction does, and insert a row into
   `verify_marker`. For a Dublin chunk that holds a repeated autumn hour, the earlier readings of
   the same station and hour are read too (also FOR SHARE): the fold rule looks back at them. They
   are context for the spec, and not compared. Which readings fall in those hours is found once,
   at the start, with the spec's own classification of hours: readings are only ever inserted,
   with the time of the insert, so a reading never moves into a past hour.
2. **Expected.** The chunk goes into an in-memory DuckDB, and `verifier.spec` builds the expected
   target rows from it, exactly as it does for the frozen check.
3. **Target side.** The marker reaches the apply service through CDC. The service applies it only
   once everything that committed before it is in the target, and copies the target's rows for
   the chunk aside in the marker's own transaction (`migration.snap_*`).
4. **Compare.** Count, then a hash over the canonical rows, then rows and columns if the hash
   differs. A chunk that differs is checked once more with a new marker, and is a finding only if
   it differs again: a difference that does not repeat was a race, one that repeats is a defect.

What FOR SHARE does not cover. MySQL's locking read takes next-key locks, so no row can be
inserted into a chunk's key range while it is read. PostgreSQL's locks only the rows that exist:
a transaction that took a key from the sequence before the read and commits after it would put a
row in the range the source side never saw. Keys come from sequences and grow, so the open end of
each table is not checked (`--margin`), and the recheck catches the rest.

The Helsinki stubs (stations only trips name) depend on every trip, not on one chunk, and are
left to the frozen check.

Writes `data/verify/continuous-<ts>.json` and exits non-zero on any finding.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

import duckdb
import psycopg
import pyarrow as pa
import pymysql

from verifier import spec
from verifier.run import LEGACY_A, LEGACY_B, OUT, TABLES, TARGET, hashed

# legacy table -> (system, local copy name, key column or None for the whole table, target table)
SOURCES = {
    "station": ("dublin", "la_station", None, "station"),
    "station_reading": ("dublin", "la_station_reading", "reading_id", "station_status"),
    "rental": ("dublin", "la_rental", "rental_id", "trip"),
    "asema": ("helsinki", "lb_asema", None, "station"),
    "matka": ("helsinki", "lb_matka", "id", "trip"),
}
EXPECTED = {
    "station": "SELECT * FROM exp_station_dublin",
    "station_reading": "SELECT * FROM exp_status",
    "rental": "SELECT * FROM exp_trip_dublin",
    "asema": "SELECT * FROM exp_station_helsinki",
    "matka": "SELECT * EXCLUDE (dep_ref, ret_ref) FROM exp_trip_helsinki",
}
TEMPLATES = {
    "la_station": "a.public.station",
    "la_station_reading": "a.public.station_reading",
    "la_rental": "a.public.rental",
    "lb_asema": "b.helsinki.asema",
    "lb_matka": "b.helsinki.matka",
}


@dataclass
class Chunk:
    table: str
    lo: int
    hi: int
    attempt: int = 1
    marker_id: str = ""
    n: int = 0  # a serial number, for the names of its DuckDB tables
    locked_ms: float = 0.0
    written_at: float = 0.0
    result: dict = field(default_factory=dict)


def _arrow(names: list[str], rows: list[tuple]) -> pa.Table:
    """Rows as an Arrow table, decimals and uuids as text: DuckDB casts them on insert."""

    def plain(v):
        return str(v) if isinstance(v, (Decimal, uuid.UUID)) else v

    cols = list(zip(*rows, strict=True)) if rows else [[] for _ in names]
    return pa.table({n: pa.array([plain(v) for v in c]) for n, c in zip(names, cols, strict=True)})


class Continuous:
    def __init__(self, chunk_size: int, margin: int):
        self.chunk_size = chunk_size
        self.margin = margin
        self.serial = 0
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone = 'UTC'")
        for ext in ("postgres", "mysql", "icu"):
            self.con.execute(f"INSTALL {ext}; LOAD {ext};")
        self.con.execute(f"ATTACH '{LEGACY_A}' AS a (TYPE postgres, READ_ONLY)")
        self.con.execute(f"ATTACH '{LEGACY_B}' AS b (TYPE mysql, READ_ONLY)")
        self.con.execute(f"ATTACH '{TARGET}' AS t (TYPE postgres, READ_ONLY)")
        # Empty tables with the frozen check's column types, cloned for every chunk.
        for local, remote in TEMPLATES.items():
            self.con.execute(f"CREATE TABLE tmpl_{local} AS SELECT * FROM {remote} LIMIT 0")
        self.con.execute(spec.MACROS)
        self.a = psycopg.connect(LEGACY_A)
        self.b = pymysql.connect(
            host="127.0.0.1",
            port=3307,
            user="helsinki",
            password="helsinki",
            database="helsinki",
            autocommit=False,
        )
        self.t = psycopg.connect(TARGET, autocommit=True)
        self.context: dict[tuple[str, int, datetime], list[int]] = {}

    def index_fold_context(self) -> None:
        """Every Dublin reading in a repeated autumn hour, by column, station and hour.

        The hours are classified by `spec.TZ_HOURS` itself, over every hour the readings span.
        """
        lo, hi = self.a.execute(
            "SELECT date_trunc('hour', least(min(reported_at), min(polled_at))), "
            "date_trunc('hour', greatest(max(reported_at), max(polled_at))) FROM station_reading"
        ).fetchone()
        self.a.rollback()
        for name in TEMPLATES:
            self.con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM tmpl_{name}")
        self.con.execute(
            "INSERT INTO la_station_reading (reading_id, station_number, reported_at, bikes, "
            "free_stands, status) SELECT row_number() OVER (), 0, h, 0, 0, '' "
            f"FROM generate_series(TIMESTAMP '{lo}', TIMESTAMP '{hi}', INTERVAL 1 HOUR) t(h)"
        )
        self.con.execute(spec.TZ_HOURS)
        hours = [
            h
            for (h,) in self.con.execute(
                "SELECT h FROM tz_hours WHERE zone = 'Europe/Dublin' AND kind = 'ambiguous'"
            ).fetchall()
        ]
        rows = self.a.execute(
            "SELECT reading_id, station_number, date_trunc('hour', reported_at), "
            "date_trunc('hour', polled_at) FROM station_reading "
            "WHERE date_trunc('hour', reported_at) = ANY(%s) "
            "OR date_trunc('hour', polled_at) = ANY(%s)",
            (hours, hours),
        ).fetchall()
        self.a.rollback()
        for reading_id, station, reported, polled in rows:
            for col, h in (("reported_at", reported), ("polled_at", polled)):
                if h in hours:
                    self.context.setdefault((col, station, h), []).append(reading_id)
        print(f"fold context: {len(rows):,} readings in {len(hours)} repeated hours", flush=True)

    # --- planning ------------------------------------------------------------------------------

    def plan(self, tables: list[str], max_chunks: int | None, start: int = 0) -> list[Chunk]:
        chunks: list[Chunk] = []
        for table in tables:
            system, _, key, _ = SOURCES[table]
            if key is None:
                chunks.append(Chunk(table, 0, 0))
                continue
            if system == "dublin":
                lo, hi = self.a.execute(f"SELECT min({key}), max({key}) FROM {table}").fetchone()
                self.a.rollback()
            else:
                with self.b.cursor() as cur:
                    cur.execute(f"SELECT min({key}), max({key}) FROM {table}")
                    lo, hi = cur.fetchone()
                self.b.rollback()
            # The open end, where keys are still being handed out, is left out.
            last = hi - self.margin
            k = max(lo, start)
            while k <= last:
                chunks.append(Chunk(table, k, min(k + self.chunk_size - 1, last)))
                k += self.chunk_size
        return chunks[:max_chunks] if max_chunks else chunks

    # --- 1. source side and marker -------------------------------------------------------------

    def mark(self, c: Chunk) -> None:
        """Read the chunk with FOR SHARE and write its marker, in one source transaction."""
        system, local, key, _ = SOURCES[c.table]
        self.serial += 1
        c.n, c.marker_id = self.serial, str(uuid.uuid4())
        where = f"WHERE {key} BETWEEN %s AND %s ORDER BY {key}" if key else ""
        params = (c.lo, c.hi) if key else ()
        started = time.monotonic()
        if system == "dublin":
            cur = self.a.cursor()
            cur.execute(f"SELECT * FROM {c.table} {where} FOR SHARE", params)
            names = [d.name for d in cur.description]
            rows = cur.fetchall()
            self._load(c, local, names, rows)
            if c.table == "station_reading":
                context = self._fold_context(cur, c)
                if context:
                    self._append(c, local, names, context)
            cur.execute("DELETE FROM verify_marker")  # the previous markers, already applied
            cur.execute(
                "INSERT INTO verify_marker (id, legacy_table, lo, hi) VALUES (%s, %s, %s, %s)",
                (c.marker_id, c.table, c.lo, c.hi),
            )
            self.a.commit()
        else:
            with self.b.cursor() as cur:
                cur.execute(f"SELECT * FROM {c.table} {where} FOR SHARE", params)
                names = [d[0] for d in cur.description]
                rows = list(cur.fetchall())
                cur.execute("DELETE FROM verify_marker")
                cur.execute(
                    "INSERT INTO verify_marker (id, legacy_table, lo, hi) VALUES (%s, %s, %s, %s)",
                    (c.marker_id, c.table, c.lo, c.hi),
                )
            self.b.commit()
            self._load(c, local, names, rows)
        c.locked_ms = round((time.monotonic() - started) * 1000, 1)
        c.written_at = time.monotonic()
        self._expected(c)

    def _load(self, c: Chunk, local: str, names: list[str], rows: list[tuple]) -> None:
        for name in TEMPLATES:
            self.con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM tmpl_{name}")
        self._append(c, local, names, rows)

    def _append(self, c: Chunk, local: str, names: list[str], rows: list[tuple]) -> None:
        chunk = _arrow(names, rows)  # noqa: F841  (read by name in the statement below)
        self.con.execute(f"INSERT INTO {local} SELECT * FROM chunk")

    def _fold_context(self, cur, c: Chunk) -> list[tuple]:
        """Earlier readings of each station in each repeated autumn hour the chunk holds."""
        self.con.execute(spec.TZ_HOURS)
        ids: set[int] = set()
        for col in ("reported_at", "polled_at"):
            hours = self.con.execute(f"""
                SELECT DISTINCT s.station_number, h.h FROM la_station_reading s
                JOIN tz_hours h ON h.zone = 'Europe/Dublin' AND h.h = date_trunc('hour', s.{col})
                WHERE h.kind = 'ambiguous'""").fetchall()
            for station, hour in hours:
                ids.update(i for i in self.context.get((col, station, hour), []) if i < c.lo)
        if not ids:
            return []
        cur.execute(
            "SELECT * FROM station_reading WHERE reading_id = ANY(%s) FOR SHARE", (sorted(ids),)
        )
        rows = cur.fetchall()
        c.result["context_rows"] = len(rows)
        return rows

    # --- 2. expected ---------------------------------------------------------------------------

    def _expected(self, c: Chunk) -> None:
        _, _, key, target = SOURCES[c.table]
        for sql in (spec.TZ_HOURS, spec.DUBLIN, spec.HELSINKI):
            self.con.execute(sql)
        src = EXPECTED[c.table]
        if key:  # leave the fold context out
            src = (
                f"SELECT * FROM ({src}) WHERE CAST(legacy_key AS BIGINT) BETWEEN {c.lo} AND {c.hi}"
            )
        pk, cols = TABLES[target]
        hashed(
            self.con, f"e_{c.n}", f"SELECT * FROM ({src}) WHERE quarantine_rule IS NULL", pk, cols
        )
        self.con.execute(
            f"CREATE OR REPLACE TABLE eq_{c.n} AS SELECT legacy_key, quarantine_rule AS rule "
            f"FROM ({src}) WHERE quarantine_rule IS NOT NULL"
        )

    # --- 3 and 4. target side and comparison ---------------------------------------------------

    def applied(self, ids: list[str]) -> set[str]:
        rows = self.t.execute(
            "SELECT marker_id::text FROM migration.verify_marker_applied "
            "WHERE marker_id = ANY(%s::uuid[])",
            (ids,),
        ).fetchall()
        return {r[0] for r in rows}

    def compare(self, c: Chunk) -> dict:
        _, _, _, target = SOURCES[c.table]
        pk, cols = TABLES[target]
        m = c.marker_id
        hashed(
            self.con,
            f"a_{c.n}",
            f"SELECT * EXCLUDE (marker_id) FROM t.migration.snap_{target} WHERE marker_id = '{m}'",
            pk,
            cols,
        )
        self.con.execute(
            f"CREATE OR REPLACE TABLE aq_{c.n} AS SELECT legacy_key, rule "
            f"FROM t.migration.snap_quarantine WHERE marker_id = '{m}'"
        )
        e, a = f"e_{c.n}", f"a_{c.n}"
        (ne, he), (na, ha) = (
            self.con.execute(
                f"SELECT count(*), sha256(string_agg(h, '' ORDER BY id)) FROM {x}"
            ).fetchone()
            for x in (e, a)
        )
        q_only_e, q_only_a = (
            self.con.execute(
                f"SELECT count(*) FROM (SELECT * FROM {x} EXCEPT SELECT * FROM {y})"
            ).fetchone()[0]
            for x, y in ((f"eq_{c.n}", f"aq_{c.n}"), (f"aq_{c.n}", f"eq_{c.n}"))
        )
        out = {"expected_rows": ne, "target_rows": na, "clean": he == ha and ne == na}
        out["clean"] = out["clean"] and q_only_e == 0 and q_only_a == 0
        if q_only_e or q_only_a:
            out["quarantine"] = {"only_expected": q_only_e, "only_target": q_only_a}
        if he != ha:
            rows = self.con.execute(f"""
                SELECT e.id, a.id,
                       {", ".join(f"e.{x} IS NOT DISTINCT FROM a.{x}" for x in cols)}
                FROM {e} e FULL JOIN {a} a ON e.id = a.id
                WHERE e.h IS DISTINCT FROM a.h""").fetchall()
            columns: dict[str, int] = {}
            for r in rows:
                if r[0] is not None and r[1] is not None:
                    for name, same in zip(cols, r[2:], strict=True):
                        if not same:
                            columns[name] = columns.get(name, 0) + 1
            out.update(
                missing=sum(1 for r in rows if r[1] is None),
                unexpected=sum(1 for r in rows if r[0] is None),
                different=sum(1 for r in rows if r[0] is not None and r[1] is not None),
                columns=columns,
                sample_ids=[str(r[0] or r[1]) for r in rows[:5]],
            )
        return out

    def cleanup(self, c: Chunk) -> None:
        for x in ("e", "a", "eq", "aq"):
            self.con.execute(f"DROP TABLE IF EXISTS {x}_{c.n}")
        for table in ("station", "station_status", "trip", "quarantine"):
            self.t.execute(
                f"DELETE FROM migration.snap_{table} WHERE marker_id = %s", (c.marker_id,)
            )


def main() -> None:
    p = argparse.ArgumentParser(prog="verifier.continuous")
    p.add_argument("--tables", nargs="*", default=list(SOURCES), choices=list(SOURCES))
    p.add_argument("--chunk", type=int, default=10_000)
    p.add_argument("--margin", type=int, default=1_000, help="keys left out at each table's end")
    p.add_argument("--in-flight", type=int, default=8, help="markers waiting at most at once")
    p.add_argument("--max-chunks", type=int, default=None)
    p.add_argument("--start", type=int, default=0, help="the first key of each table to check")
    p.add_argument("--marker-timeout", type=float, default=300)
    args = p.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    v = Continuous(args.chunk, args.margin)
    if "station_reading" in args.tables:
        v.index_fold_context()
    todo = deque(v.plan(args.tables, args.max_chunks, args.start))
    planned = len(todo)
    waiting: deque[Chunk] = deque()
    done: list[Chunk] = []
    findings: list[dict] = []
    started = time.monotonic()
    report: dict = {"started_at": datetime.now(UTC).isoformat(), "chunk_size": args.chunk}
    print(f"{planned:,} chunks over {', '.join(args.tables)}", flush=True)
    while todo or waiting:
        while todo and len(waiting) < args.in_flight:
            c = todo.popleft()
            v.mark(c)
            waiting.append(c)
        ready = v.applied([c.marker_id for c in waiting])
        progressed = False
        for c in list(waiting):
            if c.marker_id in ready:
                waiting.remove(c)
                c.result.update(
                    v.compare(c), lag_ms=round((time.monotonic() - c.written_at) * 1000)
                )
                v.cleanup(c)
                progressed = True
                if c.result["clean"]:
                    done.append(c)
                elif c.attempt == 1:
                    first = c.result
                    again = Chunk(c.table, c.lo, c.hi, attempt=2, result={"first": first})
                    todo.appendleft(again)
                else:
                    done.append(c)
                    findings.append({"table": c.table, "lo": c.lo, "hi": c.hi, **c.result})
            elif time.monotonic() - c.written_at > args.marker_timeout:
                raise TimeoutError(f"marker {c.marker_id} ({c.table} {c.lo}..{c.hi}) not applied")
        if len(done) and len(done) % 100 == 0 and progressed:
            rate = len(done) / (time.monotonic() - started)
            print(
                f"{len(done):,}/{planned:,} chunks, {rate:.1f}/s, {len(findings)} findings",
                flush=True,
            )
        if not progressed:
            time.sleep(0.05)
    elapsed = time.monotonic() - started
    by_table: dict[str, dict] = {}
    for c in done:
        t = by_table.setdefault(c.table, {"chunks": 0, "rows": 0, "rechecked": 0, "clean": 0})
        t["chunks"] += 1
        t["rows"] += c.result["expected_rows"]
        t["rechecked"] += c.attempt == 2
        t["clean"] += bool(c.result["clean"])
    locked = sorted(c.locked_ms for c in done)
    lag = sorted(c.result["lag_ms"] for c in done)

    def pct(xs, q):
        return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None

    report.update(
        tables=by_table,
        findings=findings,
        clean=not findings,
        elapsed_s=round(elapsed, 1),
        lock_ms={
            "p50": pct(locked, 0.5),
            "p99": pct(locked, 0.99),
            "max": locked[-1] if locked else None,
        },
        marker_lag_ms={
            "p50": pct(lag, 0.5),
            "p99": pct(lag, 0.99),
            "max": lag[-1] if lag else None,
        },
        finished_at=datetime.now(UTC).isoformat(),
    )
    path = OUT / f"continuous-{datetime.now(UTC):%Y%m%dT%H%M%SZ}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf8")
    print(
        json.dumps(
            {k: report[k] for k in ("tables", "clean", "elapsed_s", "lock_ms", "marker_lag_ms")},
            indent=2,
        ),
        flush=True,
    )
    print(f"findings: {len(findings)}; report: {path.name}", flush=True)
    sys.exit(0 if not findings else 1)


if __name__ == "__main__":
    main()
