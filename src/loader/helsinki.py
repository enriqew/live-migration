"""HSL files into legacy B rows.

Legacy B is loaded as HSL publishes, defects included (docs/SOURCES.md): no trip id, local time
with no zone, null and negative distances, trips that return before they depart. The loader only
does what MySQL itself would do on insert, and says so where it does:

- `2025-10-01T00:05:25` becomes `2025-10-01 00:05:25`.
- A departure published as a bare date (`2025-10-01`) becomes midnight, which is what a DATETIME
  column stores for it. The fact that the time was missing does not survive into legacy B.
- Station ids stay text, leading zeros and all. Names are not trimmed here: the leading space of
  ` Puotila` is in legacy B, and trimming is a rule of the migration (MAPPING_SPEC section 4).
"""

from __future__ import annotations

import csv
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

_DATETIME = re.compile(r"^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}:\d{2}:\d{2}))?$")
_INTEGER = re.compile(r"^-?\d+(?:\.0+)?$")
_METRES = re.compile(r"^-?\d+(?:\.\d{1,2})?$")


@dataclass
class Asema:
    id: str
    nimi: str
    namn: str | None
    name: str | None
    osoite: str | None
    adress: str | None
    kaupunki: str | None
    operaattori: str | None
    kapasiteetti: int | None
    x: str | None
    y: str | None


@dataclass
class Matka:
    departure: str
    return_: str
    departure_station_id: str
    departure_station_name: str | None
    return_station_id: str
    return_station_name: str | None
    covered_distance_m: str | None  # decimal text, up to two places
    duration_sec: int


def _rows(path: Path) -> Iterator[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        yield from csv.DictReader(fh)


def datetime_text(value: str) -> str:
    m = _DATETIME.match(value.strip())
    if not m:
        raise ValueError(f"not a date or datetime: {value!r}")
    return f"{m[1]} {m[2] or '00:00:00'}"


def integer(value: str) -> int | None:
    """An integer column, null when empty. '1517.0' is accepted, '1517.5' is not."""
    text = value.strip()
    if text == "":
        return None
    if not _INTEGER.match(text):
        raise ValueError(f"not an integer: {value!r}")
    return int(text.split(".")[0])


def metres(value: str) -> str | None:
    """A distance as published: whole metres, or metres with up to two decimals (2018 onwards).

    Kept as decimal text so nothing is rounded on the way into legacy B's decimal(10,2).
    """
    text = value.strip()
    if text == "":
        return None
    if not _METRES.match(text):
        raise ValueError(f"not a distance in metres: {value!r}")
    return text


def _opt(value: str) -> str | None:
    return value if value != "" else None


def parse_trips(path: Path) -> list[Matka]:
    trips = []
    for r in _rows(path):
        duration = integer(r["Duration (sec.)"])
        if duration is None:
            raise ValueError(f"{path.name}: trip with no duration: {r}")
        trips.append(
            Matka(
                departure=datetime_text(r["Departure"]),
                return_=datetime_text(r["Return"]),
                departure_station_id=r["Departure station id"],
                departure_station_name=_opt(r["Departure station name"]),
                return_station_id=r["Return station id"],
                return_station_name=_opt(r["Return station name"]),
                covered_distance_m=metres(r["Covered distance (m)"]),
                duration_sec=duration,
            )
        )
    return trips


def parse_stations(path: Path) -> list[Asema]:
    return [
        Asema(
            id=r["ID"],
            nimi=r["Nimi"],
            namn=_opt(r["Namn"]),
            name=_opt(r["Name"]),
            osoite=_opt(r["Osoite"]),
            adress=_opt(r["Adress"]),
            kaupunki=_opt(r["Kaupunki"]),
            operaattori=_opt(r["Operaattor"]),
            kapasiteetti=integer(r["Kapasiteet"]),
            x=_opt(r["x"]),
            y=_opt(r["y"]),
        )
        for r in _rows(path)
    ]
