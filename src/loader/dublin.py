"""Dublinbikes files, both eras, into legacy A rows.

Legacy A stores Dublin wall-clock time with no zone, the convention of the older files. The GBFS
files are UTC and are written the same way, so the repeated autumn hour becomes ambiguous in legacy
A exactly as it already is in the older data. What keeps it resolvable is `reading_id`: readings
are inserted in real-time order per station, and this module is where that order is guaranteed.

- GBFS rows carry UTC, so real order is known: they are sorted by (UTC time, station).
- JCDecaux rows carry local time only, so the file order is the only witness. It is kept, and
  checked: per station, poll time may step backwards only inside an autumn transition hour.
- Rows outside the months a file was assigned are dropped (see `parse`).
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

DUBLIN = ZoneInfo("Europe/Dublin")
UTC = ZoneInfo("UTC")
LOCAL_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass
class Station:
    number: int
    name: str
    address: str | None
    latitude: str
    longitude: str
    bike_stands: int


@dataclass
class Reading:
    station_number: int
    reported_at: str  # Dublin wall clock, 'YYYY-MM-DD HH:MM:SS'
    polled_at: str | None
    bikes: int
    free_stands: int
    status: str  # 'OPEN' | 'CLOSED'
    is_renting: bool | None
    is_returning: bool | None


class OrderError(ValueError):
    pass


def _rows(path: Path) -> Iterator[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        header = [h.strip().upper().replace(" ", "_") for h in next(reader)]
        for line in reader:
            if line:
                yield dict(zip(header, line, strict=True))


def _bool(text: str) -> bool:
    match text.strip().lower():
        case "true":
            return True
        case "false":
            return False
    raise ValueError(f"not a boolean: {text!r}")


def _status(text: str) -> str:
    value = text.strip().upper()
    if value not in ("OPEN", "CLOSED"):
        raise ValueError(f"unknown station status {text!r}")
    return value


def utc_to_local(text: str) -> str:
    utc = datetime.strptime(text, LOCAL_FORMAT).replace(tzinfo=UTC)
    return utc.astimezone(DUBLIN).strftime(LOCAL_FORMAT)


def is_autumn_repeat(local: datetime) -> bool:
    """True if this Dublin wall-clock time occurs twice (the hour clocks go back)."""
    # In a repeat, fold=0 is the earlier pass (summer time, larger offset). In the spring gap the
    # offsets also differ, the other way round, so comparing for inequality is not enough.
    first = local.replace(tzinfo=DUBLIN, fold=0).utcoffset()
    second = local.replace(tzinfo=DUBLIN, fold=1).utcoffset()
    return first > second


def parse_jcdecaux(
    path: Path, months: set[str] | None = None
) -> tuple[dict[int, Station], list[Reading], int]:
    stations: dict[int, Station] = {}
    readings: list[Reading] = []
    last_poll: dict[int, datetime] = {}
    dropped = 0
    for r in _rows(path):
        if months is not None and r["TIME"][:7] not in months:
            dropped += 1
            continue
        number = int(r["STATION_ID"])
        stations[number] = Station(
            number=number,
            name=r["NAME"],
            address=r["ADDRESS"] or None,
            latitude=r["LATITUDE"],
            longitude=r["LONGITUDE"],
            bike_stands=int(r["BIKE_STANDS"]),
        )
        polled = datetime.strptime(r["TIME"], LOCAL_FORMAT)
        previous = last_poll.get(number)
        if previous and polled < previous:
            # Allowed only when the clock went back: the later reading is in the repeated hour.
            if not (is_autumn_repeat(polled) and previous - polled <= timedelta(hours=1)):
                raise OrderError(
                    f"{path.name}: station {number} polled at {r['TIME']} after "
                    f"{previous:%Y-%m-%d %H:%M:%S}, outside an autumn transition"
                )
        last_poll[number] = polled
        readings.append(
            Reading(
                station_number=number,
                reported_at=r["LAST_UPDATED"],
                polled_at=r["TIME"],
                bikes=int(r["AVAILABLE_BIKES"]),
                free_stands=int(r["AVAILABLE_BIKE_STANDS"]),
                status=_status(r["STATUS"]),
                is_renting=None,
                is_returning=None,
            )
        )
    return stations, readings, dropped


def parse_gbfs(
    path: Path, months: set[str] | None = None
) -> tuple[dict[int, Station], list[Reading], int]:
    stations: dict[int, Station] = {}
    rows = []
    dropped = 0
    for r in _rows(path):
        if months is not None and r["LAST_REPORTED"][:7] not in months:
            dropped += 1
            continue
        number = int(r["STATION_ID"])
        stations[number] = Station(
            number=number,
            name=r["NAME"],
            address=r["ADDRESS"] or None,
            latitude=r["LAT"],
            longitude=r["LON"],
            bike_stands=int(r["CAPACITY"]),
        )
        rows.append((r["LAST_REPORTED"], number, r))
    rows.sort(key=lambda t: (t[0], t[1]))  # UTC text sorts in time order
    readings = [
        Reading(
            station_number=number,
            reported_at=utc_to_local(utc),
            polled_at=None,
            bikes=int(r["NUM_BIKES_AVAILABLE"]),
            free_stands=int(r["NUM_DOCKS_AVAILABLE"]),
            status="OPEN" if _bool(r["IS_INSTALLED"]) else "CLOSED",
            is_renting=_bool(r["IS_RENTING"]),
            is_returning=_bool(r["IS_RETURNING"]),
        )
        for utc, number, r in rows
    ]
    return stations, readings, dropped


def parse(
    path: Path, era: str, months: set[str] | None = None
) -> tuple[dict[int, Station], list[Reading], int]:
    """Stations, readings, and how many rows fell outside `months` and were dropped.

    A file's name is not proof of its content: the quarterly file named 2021-10-01 to 2022-01-01
    also carries all of 2022-01-01, which the January file carries again. Rows are kept only for
    the months the catalog assigned the file, read from the file's own clock (local poll time for
    JCDecaux, UTC for GBFS), so no month is ever loaded twice.
    """
    if era == "jcdecaux":
        return parse_jcdecaux(path, months)
    if era == "gbfs":
        return parse_gbfs(path, months)
    raise ValueError(f"unknown Dublin era {era!r}")
