"""The continuous verifier's handling of rows on their way into DuckDB."""

import uuid
from datetime import datetime
from decimal import Decimal

import duckdb

from verifier.continuous import _arrow


def test_rows_reach_duckdb_exactly_whatever_python_type_the_driver_gave():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t (id BIGINT, distance DECIMAL(10,2), x DECIMAL(16,13), ref UUID, "
        "seen TIMESTAMP, name VARCHAR)"
    )
    ref = uuid.UUID("c040d936-f655-5978-9236-51f890c6a707")
    rows = [
        (
            1,
            Decimal("1234.50"),
            Decimal("24.9384012345678"),
            ref,
            datetime(2021, 10, 31, 1, 30),
            "a",
        ),
        (2, None, None, None, None, None),
    ]
    chunk = _arrow(["id", "distance", "x", "ref", "seen", "name"], rows)  # noqa: F841
    con.execute("INSERT INTO t SELECT * FROM chunk")
    got = con.execute("SELECT * FROM t ORDER BY id").fetchall()
    # Decimals keep every digit (no float on the way), uuids and wall times are unchanged.
    assert got[0] == rows[0]
    assert got[1] == rows[1]


def test_an_empty_chunk_still_has_its_columns():
    con = duckdb.connect()
    con.execute("CREATE TABLE t (id BIGINT, name VARCHAR)")
    chunk = _arrow(["id", "name"], [])  # noqa: F841
    con.execute("INSERT INTO t SELECT * FROM chunk")
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 0
