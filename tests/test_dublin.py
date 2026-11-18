import pytest

from loader.dublin import OrderError, is_autumn_repeat, parse_gbfs, parse_jcdecaux, utc_to_local

JCD_HEADER = (
    "STATION ID,TIME,LAST UPDATED,NAME,BIKE_STANDS,AVAILABLE_BIKE_STANDS,AVAILABLE_BIKES,"
    "STATUS,ADDRESS,LATITUDE,LONGITUDE\n"
)
GBFS_HEADER = (
    '"system_id","last_reported","station_id","num_bikes_available","num_docks_available",'
    '"is_installed","is_renting","is_returning","name","short_name","address","lat","lon",'
    '"region_id","capacity"\n'
)


def write(tmp_path, name, text):
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return p


def jcd(station, time, updated, status="OPEN"):
    return (
        f"{station},{time},{updated},CLARENDON ROW,31,24,7,{status},Clarendon Row,53.3409,-6.2625\n"
    )


def test_the_older_header_with_spaces_reads_the_same(tmp_path):
    header = JCD_HEADER.replace("BIKE_STANDS", "BIKE STANDS").replace(
        "AVAILABLE_BIKE", "AVAILABLE BIKE"
    )
    p = write(
        tmp_path, "a.csv", header + jcd(1, "2018-08-01 12:30:02", "2018-08-01 12:26:05", "Open")
    )
    stations, readings, _ = parse_jcdecaux(p)
    assert stations[1].bike_stands == 31
    assert readings[0].status == "OPEN"
    assert readings[0].reported_at == "2018-08-01 12:26:05"
    assert readings[0].polled_at == "2018-08-01 12:30:02"


def test_the_repeated_autumn_hour_is_accepted_in_file_order(tmp_path):
    rows = [
        jcd(1, "2023-10-29 01:00:02", "2023-10-29 00:58:00"),
        jcd(1, "2023-10-29 01:30:02", "2023-10-29 01:28:00"),
        jcd(1, "2023-10-29 01:00:03", "2023-10-29 00:59:00"),  # clocks went back
        jcd(1, "2023-10-29 01:30:03", "2023-10-29 01:29:00"),
    ]
    _, readings, _ = parse_jcdecaux(write(tmp_path, "a.csv", JCD_HEADER + "".join(rows)))
    assert [r.polled_at[-8:] for r in readings] == ["01:00:02", "01:30:02", "01:00:03", "01:30:03"]


def test_time_going_backwards_outside_a_transition_is_refused(tmp_path):
    rows = [
        jcd(1, "2023-06-01 10:30:02", "2023-06-01 10:28:00"),
        jcd(1, "2023-06-01 10:00:02", "2023-06-01 09:58:00"),
    ]
    with pytest.raises(OrderError, match="outside an autumn transition"):
        parse_jcdecaux(write(tmp_path, "a.csv", JCD_HEADER + "".join(rows)))


def test_gbfs_is_utc_written_as_dublin_wall_clock_in_real_order(tmp_path):
    # 2025-10-26: 00:30 UTC is 01:30 summer time, 01:30 UTC is 01:30 winter time.
    tail = '"X","","X",53.34,-6.26,"",31\n'
    rows = [
        '"dublin_bikes","2025-10-26 01:30:00","1",5,26,true,true,true,' + tail,
        '"dublin_bikes","2025-10-26 00:30:00","1",6,25,true,false,true,' + tail,
    ]
    _, readings, _ = parse_gbfs(write(tmp_path, "g.csv", GBFS_HEADER + "".join(rows)))
    assert [r.reported_at for r in readings] == ["2025-10-26 01:30:00", "2025-10-26 01:30:00"]
    assert [r.bikes for r in readings] == [6, 5]  # sorted by UTC: summer time first
    assert readings[0].is_renting is False and readings[0].polled_at is None


def test_gbfs_status_comes_from_is_installed(tmp_path):
    row = '"dublin_bikes","2025-03-01 10:00:00","7",0,0,false,false,false,'
    row += '"X","","X",53.3,-6.2,"",20\n'
    _, readings, _ = parse_gbfs(write(tmp_path, "g.csv", GBFS_HEADER + row))
    assert readings[0].status == "CLOSED"


def test_utc_to_local_and_the_ambiguous_hour():
    assert utc_to_local("2025-07-01 12:00:00") == "2025-07-01 13:00:00"
    assert utc_to_local("2025-01-01 12:00:00") == "2025-01-01 12:00:00"
    from datetime import datetime

    assert is_autumn_repeat(datetime(2025, 10, 26, 1, 15))
    assert not is_autumn_repeat(datetime(2025, 10, 26, 2, 15))
    assert not is_autumn_repeat(datetime(2025, 3, 30, 1, 15))


def test_rows_outside_the_assigned_months_are_dropped_and_counted(tmp_path):
    rows = [
        jcd(1, "2021-12-31 23:55:02", "2021-12-31 23:51:00"),
        jcd(1, "2022-01-01 00:00:04", "2021-12-31 23:57:39"),  # the next file's month
    ]
    p = write(tmp_path, "q.csv", JCD_HEADER + "".join(rows))
    _, readings, dropped = parse_jcdecaux(p, {"2021-10", "2021-11", "2021-12"})
    assert [r.polled_at for r in readings] == ["2021-12-31 23:55:02"] and dropped == 1
