import pytest

from loader.catalog import Month, classify_dublin, select_dublin


def res(name: str) -> dict:
    return {"name": name, "url": f"https://example.invalid/{name}"}


def test_quarterly_end_date_is_exclusive():
    era, months = classify_dublin("Dublinbikes 2021/10/01-2022/01/01")
    assert era == "jcdecaux"
    assert [str(m) for m in months] == ["2021-10", "2021-11", "2021-12"]


def test_eras_are_told_apart_by_name():
    assert classify_dublin("DublinBike historical data 2022-06")[0] == "jcdecaux"
    assert classify_dublin("Dublin Bikes Historical Station Data June 2026")[0] == "gbfs"
    assert classify_dublin("Dublinbikes Stations GPS Co-ords") is None
    assert classify_dublin("Dublinbikes GBFS API - JCDecaux") is None


def test_overlap_takes_the_quarterly_file_and_never_a_month_twice():
    resources = [
        res("DublinBike historical data 2021-10"),
        res("DublinBike historical data 2021-11"),
        res("DublinBike historical data 2021-12"),
        res("DublinBike historical data 2022-01"),
        res("Dublinbikes 2021/10/01-2022/01/01"),
    ]
    files = select_dublin(resources, Month(2021, 10), Month(2022, 1))
    assert [f.months for f in files] == [("2021-10", "2021-11", "2021-12"), ("2022-01",)]
    assert files[0].url.endswith("2021/10/01-2022/01/01")


def test_window_cuts_files_at_both_ends():
    resources = [
        res("Dublinbikes 2021/07/01-2021/10/01"),
        res("DublinBike historical data 2022-01"),
    ]
    files = select_dublin(resources, Month(2021, 9), Month(2021, 12))
    assert [f.months for f in files] == [("2021-09",)]


def test_partial_overlap_is_refused():
    resources = [res("Dublinbikes 2021/10/01-2022/01/01"), res("Dublinbikes 2021/12/01-2022/03/01")]
    with pytest.raises(ValueError, match="partly overlaps"):
        select_dublin(resources, Month(2021, 10), Month(2022, 3))


def test_month_arithmetic_crosses_the_year():
    assert Month(2021, 12).next() == Month(2022, 1)
