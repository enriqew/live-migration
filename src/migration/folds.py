"""The fold of every Dublin reading written in an autumn hour, read from legacy A once.

MAPPING_SPEC section 3: per station, in `reading_id` order, the wall clock steps backwards once
inside the repeated hour; readings before the step are summer time (fold 0), after it winter time
(fold 1). `reported_at` and `polled_at` are resolved independently. A station with no backward
step in the hour keeps fold 0 for all its readings there, and is counted.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import psycopg

from migration.transform import DUBLIN, kind_of_wall_time


@dataclass
class FoldMap:
    folds: dict[tuple[int, str], int] = field(default_factory=dict)
    hours: list[datetime] = field(default_factory=list)
    stations_without_step: int = 0


def ambiguous_hours(first_year: int, last_year: int) -> list[datetime]:
    """The start of each repeated Dublin hour (wall clock) in [first_year, last_year]."""
    hours = []
    for year in range(first_year, last_year + 1):
        day = datetime(year, 10, 31)
        while day.weekday() != 6:  # last Sunday of October
            day -= timedelta(days=1)
        start = day.replace(hour=1)
        assert kind_of_wall_time(start, DUBLIN) == "ambiguous", start
        hours.append(start)
    return hours


def assign(rows: list[tuple[int, int, datetime]]) -> tuple[dict[int, int], int]:
    """Folds for (station, reading_id, wall) rows of one hour and one column, in reading_id order.

    Returns the folds by reading_id and how many stations showed no backward step.
    """
    folds: dict[int, int] = {}
    without_step = 0
    by_station: dict[int, list[tuple[int, datetime]]] = {}
    for station, reading_id, wall in rows:
        by_station.setdefault(station, []).append((reading_id, wall))
    for readings in by_station.values():
        readings.sort()
        fold, previous, stepped = 0, None, False
        for reading_id, wall in readings:
            if previous is not None and wall < previous and not stepped:
                fold, stepped = 1, True
            folds[reading_id] = fold
            previous = wall
        if not stepped:
            without_step += 1
    return folds, without_step


def load(dsn: str) -> FoldMap:
    result = FoldMap()
    with psycopg.connect(dsn) as conn:
        span = conn.execute(
            "SELECT extract(year FROM min(reported_at))::int, "
            "extract(year FROM max(reported_at))::int FROM station_reading"
        ).fetchone()
        if span[0] is None:
            return result
        result.hours = ambiguous_hours(span[0], span[1])
        for start in result.hours:
            end = start + timedelta(hours=1)
            for column in ("reported_at", "polled_at"):
                rows = conn.execute(
                    f"SELECT station_number, reading_id, {column} FROM station_reading "
                    f"WHERE {column} >= %s AND {column} < %s",
                    (start, end),
                ).fetchall()
                folds, without_step = assign(rows)
                result.folds.update({(rid, column): f for rid, f in folds.items()})
                result.stations_without_step += without_step
    return result
