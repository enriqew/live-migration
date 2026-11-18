import unicodedata
from datetime import UTC, datetime
from decimal import Decimal

from migration.transform import (
    DUBLIN,
    Quarantine,
    clean_name,
    dublin_reading,
    dublin_rental,
    helsinki_station,
    helsinki_station_stub,
    helsinki_trip,
    kind_of_wall_time,
    round_coordinate,
    target_id,
)


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def test_ids_match_the_value_checked_in_postgres():
    assert str(target_id("dublin", "station", 42)) == "926653f3-c058-5793-b863-554e4c585425"


def test_rounding_is_half_away_from_zero_on_ties():
    assert round_coordinate("24.0000005") == Decimal("24.000001")  # half to even gives 24.000000
    assert round_coordinate("-24.0000005") == Decimal("-24.000001")
    assert round_coordinate("60.1842832921200") == Decimal("60.184283")


def test_names_are_trimmed_and_nfc_and_nothing_else():
    decomposed = unicodedata.normalize("NFD", "Sörnäinen (M)")
    assert clean_name(" Puotila") == "Puotila"
    assert clean_name(decomposed) == "Sörnäinen (M)"
    assert clean_name("CLARENDON ROW") == "CLARENDON ROW"


def test_wall_time_kinds_in_dublin():
    assert kind_of_wall_time(datetime(2025, 10, 26, 1, 30), DUBLIN) == "ambiguous"
    assert kind_of_wall_time(datetime(2025, 3, 30, 1, 30), DUBLIN) == "nonexistent"
    assert kind_of_wall_time(datetime(2025, 7, 1, 12, 0), DUBLIN) == "single"


def reading(reading_id, reported, polled=None):
    return {
        "reading_id": reading_id,
        "station_number": 1,
        "reported_at": reported,
        "polled_at": polled,
        "bikes": 3,
        "free_stands": 28,
        "status": "OPEN",
        "is_renting": True,
        "is_returning": True,
    }


def test_an_ambiguous_dublin_reading_takes_its_fold_from_the_map_or_is_quarantined():
    wall = datetime(2025, 10, 26, 1, 30)
    summer = dublin_reading(reading(10, wall), {(10, "reported_at"): 0})
    winter = dublin_reading(reading(11, wall), {(11, "reported_at"): 1})
    assert summer["reported_at"] == utc(2025, 10, 26, 0, 30)
    assert winter["reported_at"] == utc(2025, 10, 26, 1, 30)
    missing = dublin_reading(reading(12, wall), {})
    assert missing == Quarantine("ambiguous_local_time", "reported_at 2025-10-26T01:30:00")


def test_a_rental_still_out_is_flagged_and_has_no_return():
    row = {
        "rental_id": 5,
        "from_station": 1,
        "to_station": None,
        "started": datetime(2026, 11, 18, 10, 0),
        "ended": None,
    }
    trip = dublin_rental(row)
    assert trip["flags"] == ["open_rental"] and trip["return_station_id"] is None
    assert trip["departed_at"] == utc(2026, 11, 18, 10, 0)


def trip(departure, ret, duration, distance=1000):
    return {
        "id": 1,
        "departure": departure,
        "return": ret,
        "departure_station_id": "043",
        "return_station_id": "A04",
        "covered_distance_m": distance,
        "duration_sec": duration,
    }


def test_helsinki_folds_are_chosen_by_the_published_duration():
    # Clocks go back at 04:00 EEST to 03:00 EET on 2025-10-26. Departing 03:55 summer time and
    # returning 03:05 winter time is ten minutes; only one fold pair says so.
    t = helsinki_trip(trip(datetime(2025, 10, 26, 3, 55), datetime(2025, 10, 26, 3, 5), 600))
    assert t["departed_at"] == utc(2025, 10, 26, 0, 55)
    assert t["returned_at"] == utc(2025, 10, 26, 1, 5)
    assert t["flags"] == []


def test_a_helsinki_tie_takes_fold_zero_and_says_so():
    t = helsinki_trip(trip(datetime(2025, 10, 26, 3, 30), datetime(2025, 10, 26, 3, 40), 600))
    assert t["departed_at"] == utc(2025, 10, 26, 0, 30)
    assert t["flags"] == ["ambiguous_time"]


def test_helsinki_defects_become_flags_not_guesses():
    t = helsinki_trip(trip(datetime(2025, 7, 31, 23, 50), datetime(2025, 7, 31, 23, 40), 323, -12))
    assert t["distance_m"] is None
    assert t["flags"] == ["negative_distance", "returned_before_departed"]
    gap = helsinki_trip(trip(datetime(2025, 3, 30, 3, 30), datetime(2025, 3, 30, 4, 40), 600))
    assert isinstance(gap, Quarantine) and gap.rule == "nonexistent_local_time"


def test_helsinki_station_kinds():
    base = {
        "nimi": " Puotila",
        "namn": None,
        "name": None,
        "osoite": None,
        "kaupunki": None,
        "kapasiteetti": None,
        "x": None,
        "y": None,
    }
    assert helsinki_station({**base, "id": "*40"})["kind"] == "service"
    assert helsinki_station({**base, "id": "043"})["kind"] == "public"
    stub = helsinki_station_stub("A04")
    assert (stub["kind"], stub["name"]) == ("unlisted", "A04")
    assert stub["station_id"] == helsinki_station({**base, "id": "A04"})["station_id"]
