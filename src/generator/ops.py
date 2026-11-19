"""What a user of each system does, as a seeded stream of operations."""

from __future__ import annotations

import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field


@dataclass
class Op:
    seq: int
    system: str  # 'dublin' | 'helsinki'
    kind: str
    method: str
    path: str
    body: dict | None
    key: str
    # Filled by the runner from the response, for operations that create something.
    on_success: str | None = None
    # The open rental a return or an erase takes off the open list; it goes back on the list
    # if the operation is not accepted, or the bike would stay out for the rest of the run.
    rental: int | None = None


@dataclass
class World:
    """What the generator knows about each system: enough to send plausible requests."""

    dublin_stations: list[int]
    helsinki_stations: list[str]
    helsinki_max_trip_id: int
    open_rentals: list[int] = field(default_factory=list)
    returned_rentals: list[int] = field(default_factory=list)
    own_trips: list[int] = field(default_factory=list)
    out_of_service: set[int] = field(default_factory=set)


# Relative weights. Inserts dominate, as in the real systems; updates and deletes are rare but
# always present, because a migration that only ever sees inserts proves very little.
# Rents and returns balance, and an erasure only ever removes a rental already returned: erasing
# one still out would take its bike out of the city for good, and over a run of hours Dublin
# would run out of bikes (it did, on 2026-11-18).
DUBLIN_MIX = {"rent": 43, "return": 43, "service": 6, "rename": 2, "capacity": 2, "erase": 4}
HELSINKI_MIX = {"trip": 80, "correct": 9, "erase": 5, "rename": 3, "capacity": 3}


class OpStream:
    def __init__(self, seed: int, run_id: str, world: World, share_dublin: float = 0.5):
        self.rng = random.Random(seed)
        self.world = world
        self.share_dublin = share_dublin
        self.seq = 0
        # One key per operation, the same across its retries, never shared between runs.
        self.key_ns = uuid.uuid5(uuid.NAMESPACE_URL, f"live-migration/run/{run_id}")

    def _key(self) -> str:
        return str(uuid.uuid5(self.key_ns, str(self.seq)))

    def __iter__(self) -> Iterator[Op]:
        while True:
            yield self.next()

    def next(self) -> Op:
        self.seq += 1
        if self.rng.random() < self.share_dublin:
            return self._dublin()
        return self._helsinki()

    def _pick(self, mix: dict[str, int]) -> str:
        return self.rng.choices(list(mix), weights=list(mix.values()))[0]

    def _dublin(self) -> Op:
        w, rng = self.world, self.rng
        kind = self._pick(DUBLIN_MIX)
        if (kind == "return" and not w.open_rentals) or (
            kind == "erase" and not w.returned_rentals
        ):
            kind = "rent"
        station = rng.choice(w.dublin_stations)
        match kind:
            case "rent":
                return self._op(
                    "dublin",
                    kind,
                    "POST",
                    "/rentals",
                    {"from_station": station},
                    on_success="open_rental",
                )
            case "return":
                rental = w.open_rentals.pop(rng.randrange(len(w.open_rentals)))
                op = self._op(
                    "dublin", kind, "POST", f"/rentals/{rental}/return", {"to_station": station}
                )
                op.rental = rental
                return op
            case "erase":
                rental = w.returned_rentals.pop(rng.randrange(len(w.returned_rentals)))
                op = self._op("dublin", kind, "DELETE", f"/rentals/{rental}", None)
                op.rental = rental
                return op
            case "service":
                # Stations go out of service and come back. At most 5% are out at once, and the
                # generator always knows which, or random toggles would close the city over a run.
                cap = max(1, len(w.dublin_stations) // 20)
                if w.out_of_service and (len(w.out_of_service) >= cap or rng.random() < 0.5):
                    station = rng.choice(sorted(w.out_of_service))
                    w.out_of_service.discard(station)
                    body = {"status": "OPEN", "is_renting": True, "is_returning": True}
                else:
                    candidates = [n for n in w.dublin_stations if n not in w.out_of_service]
                    station = rng.choice(candidates)
                    w.out_of_service.add(station)
                    body = rng.choice(
                        [{"is_renting": False}, {"is_returning": False}, {"status": "CLOSED"}]
                    )
                return self._op("dublin", kind, "POST", f"/stations/{station}/service", body)
            case "rename":
                body = {
                    "name": f"STATION {station} {rng.choice(['NORTH', 'SOUTH', 'EAST', 'WEST'])}"
                }
                return self._op("dublin", kind, "PATCH", f"/stations/{station}", body)
            case _:
                body = {"bike_stands": rng.randint(10, 40)}
                return self._op("dublin", kind, "PATCH", f"/stations/{station}", body)

    def _helsinki(self) -> Op:
        w, rng = self.world, self.rng
        kind = self._pick(HELSINKI_MIX)
        match kind:
            case "trip":
                duration = int(rng.lognormvariate(6.4, 0.6))  # median about 10 minutes
                distance = None if rng.random() < 0.001 else int(duration * rng.uniform(2.5, 4.5))
                body = {
                    "departure_station_id": rng.choice(w.helsinki_stations),
                    "return_station_id": rng.choice(w.helsinki_stations),
                    "duration_sec": duration,
                    "covered_distance_m": distance,
                }
                return self._op("helsinki", kind, "POST", "/matkat", body, on_success="own_trip")
            case "correct" | "erase":
                # Mostly historic rows, so that changes land all over the key space, including
                # chunks a snapshot has already copied; sometimes the generator's own trips.
                if w.own_trips and rng.random() < 0.3:
                    trip = rng.choice(w.own_trips)
                else:
                    trip = rng.randint(1, max(1, w.helsinki_max_trip_id))
                if kind == "erase":
                    return self._op("helsinki", kind, "DELETE", f"/matkat/{trip}", None)
                body = rng.choice(
                    [
                        {"covered_distance_m": rng.randint(100, 9000)},
                        {"duration_sec": rng.randint(60, 3600)},
                    ]
                )
                return self._op("helsinki", kind, "PATCH", f"/matkat/{trip}", body)
            case "rename":
                station = rng.choice(w.helsinki_stations)
                body = {
                    "nimi": f"Asema {station} {rng.choice(['pohjoinen', 'etelä', 'itä', 'länsi'])}"
                }
                return self._op("helsinki", kind, "PATCH", f"/asemat/{station}", body)
            case _:
                station = rng.choice(w.helsinki_stations)
                body = {"kapasiteetti": rng.randint(8, 40)}
                return self._op("helsinki", kind, "PATCH", f"/asemat/{station}", body)

    def _op(self, system, kind, method, path, body, on_success=None) -> Op:
        return Op(self.seq, system, kind, method, path, body, self._key(), on_success)
