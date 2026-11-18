"""Legacy rows to target rows: the first implementation of docs/MAPPING_SPEC.md.

Pure functions. Each takes one legacy row, as the change stream delivers it once decoded (wall
clock times as naive datetimes, decimals as their text), and returns either the target row or a
Quarantine saying which rule it breaks. Nothing here reads a database or remembers anything
between calls: the one thing the spec cannot decide from a single row, the fold of an ambiguous
Dublin reading, comes in as a map computed from legacy A beforehand (`folds`).

The verifier implements the same spec in SQL and never imports this package.
"""

from __future__ import annotations

import unicodedata
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

NAMESPACE = uuid.UUID("c040d936-f655-5978-9236-51f890c6a707")
DUBLIN = ZoneInfo("Europe/Dublin")
HELSINKI = ZoneInfo("Europe/Helsinki")
SIX_PLACES = Decimal("0.000001")
HELSINKI_SERVICE_STATIONS = frozenset({"900", "902", "997", "999", "*40"})


@dataclass(frozen=True)
class Quarantine:
    rule: str
    detail: str = ""


def target_id(system: str, table: str, key: object) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE, f"{system}:{table}:{key}")


ASCII_WHITESPACE = " \t\n\r\f\v"


def clean_name(text: str | None) -> str | None:
    """Trimmed of ASCII whitespace, then NFC; a value that is empty once trimmed is null.

    ASCII only, as the spec says: `str.strip()` with no argument would also strip a no-break
    space or an ideographic space, which the verifier's SQL does not, and the two would disagree
    the day one appeared.
    """
    if text is None:
        return None
    return unicodedata.normalize("NFC", text.strip(ASCII_WHITESPACE)) or None


def round_coordinate(text: str | None) -> Decimal | None:
    """Six decimals, half away from zero, from the decimal text, never through a float.

    Decimal's default is half to even; ROUND_HALF_UP is half away from zero, which is what
    PostgreSQL's round(numeric) does. The two implementations disagree exactly on ties otherwise.
    """
    if text is None:
        return None
    return Decimal(text).quantize(SIX_PLACES, rounding=ROUND_HALF_UP)


# --- time ---------------------------------------------------------------------------------------


def kind_of_wall_time(wall: datetime, zone: ZoneInfo) -> str:
    """'single', 'ambiguous' (the autumn hour) or 'nonexistent' (the spring gap)."""
    first = wall.replace(tzinfo=zone, fold=0).utcoffset()
    second = wall.replace(tzinfo=zone, fold=1).utcoffset()
    if first == second:
        return "single"
    return "ambiguous" if first > second else "nonexistent"


def to_utc(wall: datetime, zone: ZoneInfo, fold: int = 0) -> datetime:
    return wall.replace(tzinfo=zone, fold=fold).astimezone(UTC)


def _single_or_quarantine(wall: datetime, zone: ZoneInfo, what: str) -> datetime | Quarantine:
    match kind_of_wall_time(wall, zone):
        case "single":
            return to_utc(wall, zone)
        case "ambiguous":
            return Quarantine("ambiguous_local_time", f"{what} {wall.isoformat()}")
        case _:
            return Quarantine("nonexistent_local_time", f"{what} {wall.isoformat()}")


# --- Dublin -------------------------------------------------------------------------------------


def dublin_station(row: dict) -> dict | Quarantine:
    if clean_name(row["name"]) is None:
        return Quarantine("blank_name", repr(row["name"]))
    return {
        "station_id": target_id("dublin", "station", row["number"]),
        "system_code": "dublin",
        "source_ref": str(row["number"]),
        "name": clean_name(row["name"]),
        "name_sv": None,
        "name_en": None,
        "address": clean_name(row["address"]),
        "city": None,
        "capacity": row["bike_stands"],
        "latitude": round_coordinate(row["latitude"]),
        "longitude": round_coordinate(row["longitude"]),
        "kind": "public",
    }


def _dublin_reading_time(
    wall: datetime | None, reading_id: int, column: str, folds: dict
) -> datetime | None | Quarantine:
    if wall is None:
        return None
    match kind_of_wall_time(wall, DUBLIN):
        case "single":
            return to_utc(wall, DUBLIN)
        case "ambiguous":
            fold = folds.get((reading_id, column))
            if fold is None:
                return Quarantine("ambiguous_local_time", f"{column} {wall.isoformat()}")
            return to_utc(wall, DUBLIN, fold)
        case _:
            return Quarantine("nonexistent_local_time", f"{column} {wall.isoformat()}")


def dublin_reading(row: dict, folds: dict[tuple[int, str], int]) -> dict | Quarantine:
    reading_id = row["reading_id"]
    reported = _dublin_reading_time(row["reported_at"], reading_id, "reported_at", folds)
    polled = _dublin_reading_time(row["polled_at"], reading_id, "polled_at", folds)
    for value in (reported, polled):
        if isinstance(value, Quarantine):
            return value
    if row["bikes"] < 0 or row["free_stands"] < 0:
        return Quarantine("negative_count", f"bikes {row['bikes']}, free {row['free_stands']}")
    return {
        "status_id": target_id("dublin", "station_reading", reading_id),
        "station_id": target_id("dublin", "station", row["station_number"]),
        "reported_at": reported,
        "polled_at": polled,
        "bikes_available": row["bikes"],
        "docks_available": row["free_stands"],
        "is_open": row["status"] == "OPEN",
        "is_renting": row["is_renting"],
        "is_returning": row["is_returning"],
    }


def dublin_rental(row: dict) -> dict | Quarantine:
    started = _single_or_quarantine(row["started"], DUBLIN, "started")
    if isinstance(started, Quarantine):
        return started
    ended = None
    if row["ended"] is not None:
        ended = _single_or_quarantine(row["ended"], DUBLIN, "ended")
        if isinstance(ended, Quarantine):
            return ended
    flags = []
    if row["to_station"] is None:
        flags.append("open_rental")
    if ended is not None and ended < started:
        flags.append("returned_before_departed")
    return {
        "trip_id": target_id("dublin", "rental", row["rental_id"]),
        "system_code": "dublin",
        "departure_station_id": target_id("dublin", "station", row["from_station"]),
        "return_station_id": (
            target_id("dublin", "station", row["to_station"])
            if row["to_station"] is not None
            else None
        ),
        "departed_at": started,
        "returned_at": ended,
        "duration_s": int((ended - started).total_seconds()) if ended is not None else None,
        "distance_m": None,
        "flags": sorted(flags),
    }


# --- Helsinki -----------------------------------------------------------------------------------


def helsinki_station(row: dict) -> dict | Quarantine:
    station_id = row["id"]
    if clean_name(row["nimi"]) is None:
        return Quarantine("blank_name", repr(row["nimi"]))
    return {
        "station_id": target_id("helsinki", "asema", station_id),
        "system_code": "helsinki",
        "source_ref": station_id,
        "name": clean_name(row["nimi"]),
        "name_sv": clean_name(row["namn"]),
        "name_en": clean_name(row["name"]),
        "address": clean_name(row["osoite"]),
        "city": clean_name(row["kaupunki"]),
        "capacity": row["kapasiteetti"],
        "latitude": round_coordinate(row["y"]),
        "longitude": round_coordinate(row["x"]),
        "kind": "service" if station_id in HELSINKI_SERVICE_STATIONS else "public",
    }


def helsinki_station_stub(station_id: str) -> dict:
    """A station trips name and `asema` does not have (spec section 4)."""
    return {
        "station_id": target_id("helsinki", "asema", station_id),
        "system_code": "helsinki",
        "source_ref": station_id,
        "name": station_id,
        "name_sv": None,
        "name_en": None,
        "address": None,
        "city": None,
        "capacity": None,
        "latitude": None,
        "longitude": None,
        "kind": "unlisted",
    }


def _helsinki_folds(departure: datetime, returned: datetime, duration: int | None):
    """The (fold departure, fold return) the spec picks, and whether it was undecided."""
    kinds = (kind_of_wall_time(departure, HELSINKI), kind_of_wall_time(returned, HELSINKI))
    if "nonexistent" in kinds:
        return None, False
    if duration is None:
        # Nothing to compare against: fold 0, and undecided if anything was ambiguous.
        return (0, 0), "ambiguous" in kinds
    dep_folds = (0, 1) if kinds[0] == "ambiguous" else (0,)
    ret_folds = (0, 1) if kinds[1] == "ambiguous" else (0,)
    scored = []
    for fd in dep_folds:
        for fr in ret_folds:
            elapsed = (
                to_utc(returned, HELSINKI, fr) - to_utc(departure, HELSINKI, fd)
            ).total_seconds()
            scored.append((abs(elapsed - duration), fd, fr))
    scored.sort()
    tie = len(scored) > 1 and scored[0][0] == scored[1][0]
    if tie:
        return (0, 0), True
    return (scored[0][1], scored[0][2]), False


def _station_ref(station_id: str | None) -> uuid.UUID | None:
    return target_id("helsinki", "asema", station_id) if station_id is not None else None


def helsinki_trip(row: dict) -> dict | Quarantine:
    departure, returned = row["departure"], row["return"]
    folds, tie = _helsinki_folds(departure, returned, row["duration_sec"])
    if folds is None:
        return Quarantine(
            "nonexistent_local_time", f"{departure.isoformat()} {returned.isoformat()}"
        )
    departed_at = to_utc(departure, HELSINKI, folds[0])
    returned_at = to_utc(returned, HELSINKI, folds[1])
    ambiguous = "ambiguous" in (
        kind_of_wall_time(departure, HELSINKI),
        kind_of_wall_time(returned, HELSINKI),
    )
    flags = []
    if ambiguous and tie:
        flags.append("ambiguous_time")
    distance = Decimal(row["covered_distance_m"]) if row["covered_distance_m"] is not None else None
    if distance is not None and distance < 0:
        distance = None
        flags.append("negative_distance")
    if returned_at < departed_at:
        flags.append("returned_before_departed")
    if row["departure_station_id"] is None:
        flags.append("missing_departure_station")
    if row["return_station_id"] is None:
        flags.append("missing_return_station")
    return {
        "trip_id": target_id("helsinki", "matka", row["id"]),
        "system_code": "helsinki",
        "departure_station_id": _station_ref(row["departure_station_id"]),
        "return_station_id": _station_ref(row["return_station_id"]),
        "departed_at": departed_at,
        "returned_at": returned_at,
        "duration_s": row["duration_sec"],
        "distance_m": distance,
        "flags": sorted(flags),
    }
