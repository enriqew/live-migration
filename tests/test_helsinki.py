import pytest

from loader.helsinki import datetime_text, integer, parse_stations, parse_trips

TRIPS_HEADER = (
    "﻿Departure,Return,Departure station id,Departure station name,Return station id,"
    "Return station name,Covered distance (m),Duration (sec.)\n"
)


def write(tmp_path, text):
    p = tmp_path / "f.csv"
    p.write_text(text, encoding="utf-8")
    return p


def test_trips_keep_their_defects(tmp_path):
    rows = [
        "2025-10-01,2025-10-01T00:05:25,043,Karhupuisto,044,Sörnäinen (M),712,224",
        "2025-07-31T23:59:58,2025-08-01T00:08:37,*40, Puotila,121,Vilhonvuorenkatu,,516",
        "2025-07-31T23:50:00,2025-07-31T23:40:00,006,Hietalahdentori,036,Apollonkatu,-12,323",
    ]
    trips = parse_trips(write(tmp_path, TRIPS_HEADER + "\n".join(rows) + "\n"))
    # A bare date is midnight, as MySQL stores it.
    assert trips[0].departure == "2025-10-01 00:00:00"
    assert trips[0].departure_station_id == "043"  # leading zero kept
    assert trips[0].return_station_name == "Sörnäinen (M)"
    assert trips[1].departure_station_name == " Puotila"  # not trimmed in legacy B
    assert trips[1].covered_distance_m is None
    assert trips[2].covered_distance_m == -12
    assert trips[2].return_ < trips[2].departure  # returned before departed, kept as published


def test_integer_accepts_whole_decimals_only():
    assert integer("1517") == 1517
    assert integer("1517.0") == 1517
    assert integer(" ") is None
    with pytest.raises(ValueError):
        integer("1517.5")


def test_datetime_text_refuses_anything_else():
    assert datetime_text("2016-07-31T23:56:00") == "2016-07-31 23:56:00"
    with pytest.raises(ValueError):
        datetime_text("31.7.2016 23:56")


def test_stations_map_the_finnish_columns(tmp_path):
    text = (
        "﻿FID,ID,Nimi,Namn,Name,Osoite,Adress,Kaupunki,Stad,Operaattor,Kapasiteet,x,y,x2,y2\n"
        "1,501,Hanasaari,Hanaholmen,Hanasaari,Hanasaarenranta 1,Hanaholmsstranden 1,Espoo,Esbo,"
        "CityBike Finland,10,24.840319,60.16582,25491134.43,6672558.15\n"
    )
    (s,) = parse_stations(write(tmp_path, text))
    assert (s.id, s.nimi, s.namn, s.kaupunki, s.kapasiteetti) == (
        "501",
        "Hanasaari",
        "Hanaholmen",
        "Espoo",
        10,
    )
    assert (s.x, s.y) == ("24.840319", "60.16582")
