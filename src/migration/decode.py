"""Debezium events, as Avro through Apicurio Registry, into plain legacy rows.

The converter writes each message as a magic byte, the schema's id in the registry, and the Avro
body. Schemas are fetched by id once and cached. Debezium's semantic types ride on the Avro schema
as `connect.name`, and are turned into Python values here:

- `io.debezium.time.MicroTimestamp` / `Timestamp`: a column without a time zone, encoded as
  micro/milliseconds since the epoch *of the wall clock*. It becomes a naive datetime: the wall
  clock the legacy system stored, which the transform then places in its zone.
- decimals arrive as text (`decimal.handling.mode=string`) and stay text: the transform rounds
  them from the decimal value, never through a float.
"""

from __future__ import annotations

import io
import json
import struct
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta

import fastavro

EPOCH = datetime(1970, 1, 1)


@dataclass(frozen=True)
class Event:
    topic: str
    partition: int
    offset: int
    op: str  # 'c' create, 'u' update, 'd' delete, 'r' read (snapshot)
    before: dict | None
    after: dict | None
    source: dict
    transaction: dict | None


class Registry:
    """Schemas by the id in each message, with Apicurio's artifact references resolved.

    The converter writes a magic byte and a 4-byte content id. It registers every nested record
    (the row, `source`, `transaction`) as an artifact of its own, which the envelope references by
    name; those are fetched and parsed first, recursively, so the envelope can be parsed against
    them. A table whose schema changed has a new version of its row artifact, and messages carry
    the content id of the envelope that references the version they were written with.
    """

    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self._by_id: dict[int, tuple[dict, dict[str, str]]] = {}
        self._raw: dict[tuple[str, str], dict] = {}

    def _get(self, path: str):
        with urllib.request.urlopen(f"{self.url}{path}", timeout=30) as resp:
            return json.load(resp)

    def _resolve(self, references: list[dict], named: dict, raw_by_name: dict) -> None:
        for ref in references:
            key = (ref["artifactId"], ref["version"])
            if key not in self._raw:
                base = f"/groups/default/artifacts/{ref['artifactId']}/versions/{ref['version']}"
                self._raw[key] = (self._get(f"{base}/content"), self._get(f"{base}/references"))
            raw, nested = self._raw[key]
            self._resolve(nested, named, raw_by_name)
            fastavro.parse_schema(raw, named_schemas=named)
            raw_by_name[_full_name(raw)] = raw

    def schema(self, content_id: int) -> tuple[dict, dict[str, str]]:
        """The parsed envelope schema and, per row column, its Debezium semantic type."""
        if content_id not in self._by_id:
            raw = self._get(f"/ids/contentIds/{content_id}")
            named: dict = {}
            raw_by_name: dict = {}
            self._resolve(self._get(f"/ids/contentIds/{content_id}/references"), named, raw_by_name)
            parsed = fastavro.parse_schema(raw, named_schemas=named)
            self._by_id[content_id] = (parsed, _semantic_types(raw, raw_by_name))
        return self._by_id[content_id]

    def decode(self, payload: bytes) -> tuple[dict, dict[str, str]]:
        if payload[0] != 0:
            raise ValueError(f"unexpected magic byte {payload[0]}")
        (content_id,) = struct.unpack(">i", payload[1:5])
        parsed, semantic = self.schema(content_id)
        record = fastavro.schemaless_reader(io.BytesIO(payload[5:]), parsed)
        return record, semantic


def _full_name(schema: dict) -> str:
    ns = schema.get("namespace")
    return f"{ns}.{schema['name']}" if ns else schema["name"]


def _semantic_types(envelope: dict, raw_by_name: dict) -> dict[str, str]:
    """Column name to `connect.name` for the row record the envelope's `after` refers to."""
    after = next((f for f in envelope["fields"] if f["name"] == "after"), None)
    if after is None:  # not a row envelope: a transaction BEGIN/END record
        return {}
    ref = next(t for t in after["type"] if t != "null")
    if isinstance(ref, str):
        full = ref if "." in ref else f"{envelope.get('namespace')}.{ref}"
        ref = raw_by_name[full]
    out: dict[str, str] = {}
    for column in ref["fields"]:
        types = column["type"] if isinstance(column["type"], list) else [column["type"]]
        for t in types:
            if isinstance(t, dict) and "connect.name" in t:
                out[column["name"]] = t["connect.name"]
    return out


def _convert(value, semantic: str | None):
    if value is None or semantic is None:
        return value
    if semantic == "io.debezium.time.MicroTimestamp":
        return EPOCH + timedelta(microseconds=value)
    if semantic == "io.debezium.time.Timestamp":
        return EPOCH + timedelta(milliseconds=value)
    return value


def row(record: dict | None, semantic: dict[str, str]) -> dict | None:
    if record is None:
        return None
    return {k: _convert(v, semantic.get(k)) for k, v in record.items()}


def event(topic: str, partition: int, offset: int, record: dict, semantic: dict) -> Event:
    return Event(
        topic=topic,
        partition=partition,
        offset=offset,
        op=record["op"],
        before=row(record.get("before"), semantic),
        after=row(record.get("after"), semantic),
        source=record["source"],
        transaction=record.get("transaction"),
    )
