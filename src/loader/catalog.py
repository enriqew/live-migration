"""Which public files make up the load, decided from the publishers' own listings.

Dublin lists its files in a CKAN package; their names are the only place the covered period is
written, so they are parsed here. Helsinki publishes one file per month at a fixed URL pattern,
and the season has no fixed start, so a month is in the catalog only if the file exists.

The catalog is a plan, not a download: `fetch` turns it into files and a lock with their hashes.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass

USER_AGENT = "live-migration-loader/0.1"
DUBLIN_PACKAGE = "https://data.gov.ie/api/3/action/package_show?id=dublinbikes-api"
HELSINKI_TRIPS = "https://dev.hsl.fi/citybikes/od-trips-{year}/{year}-{month:02d}.csv"
HELSINKI_STATIONS = "https://opendata.arcgis.com/datasets/726277c507ef4914b0aec3cbcfcbfafc_0.csv"

MONTH_NAMES = (
    "January February March April May June July August September October November December"
).split()
MONTHS = {name: i for i, name in enumerate(MONTH_NAMES, start=1)}

_QUARTERLY = re.compile(r"^Dublinbikes (\d{4})/(\d{2})/01-(\d{4})/(\d{2})/01$")
_MONTHLY = re.compile(r"^DublinBike historical data (\d{4})-(\d{2})$")
_GBFS = re.compile(r"^Dublin Bikes Historical Station Data (\w+) (\d{4})$")


@dataclass(frozen=True, order=True)
class Month:
    year: int
    month: int

    @classmethod
    def parse(cls, text: str) -> Month:
        year, month = text.split("-")
        return cls(int(year), int(month))

    def next(self) -> Month:
        return Month(self.year + self.month // 12, self.month % 12 + 1)

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


@dataclass(frozen=True)
class SourceFile:
    system: str  # 'dublin' | 'helsinki'
    kind: str  # 'readings' | 'trips' | 'stations'
    era: str  # file format: 'jcdecaux' | 'gbfs' | 'hsl'
    name: str  # local file name, unique within the system
    url: str
    months: tuple[str, ...]  # months the file covers, 'YYYY-MM'


def months_between(first: Month, last: Month) -> list[Month]:
    out, m = [], first
    while m <= last:
        out.append(m)
        m = m.next()
    return out


def classify_dublin(resource_name: str) -> tuple[str, list[Month]] | None:
    """The era and the covered months of a Dublin resource, or None if it is not a readings file."""
    name = resource_name.strip()
    if m := _QUARTERLY.match(name):
        start, end = Month(int(m[1]), int(m[2])), Month(int(m[3]), int(m[4]))
        return "jcdecaux", months_between(start, end)[:-1]  # the end date is exclusive
    if m := _MONTHLY.match(name):
        return "jcdecaux", [Month(int(m[1]), int(m[2]))]
    if (m := _GBFS.match(name)) and m[1] in MONTHS:
        return "gbfs", [Month(int(m[2]), MONTHS[m[1]])]
    return None


def select_dublin(resources: list[dict], first: Month, last: Month) -> list[SourceFile]:
    """Readings files covering [first, last], one file per month.

    Where a quarterly file and a monthly file cover the same month (2021-10 to 2021-12), the
    quarterly one is taken: its cadence is 5 minutes against 30. A month is never loaded twice.
    """
    candidates = []
    for r in resources:
        parsed = classify_dublin(r.get("name", ""))
        if parsed is None:
            continue
        era, months = parsed
        months = [m for m in months if first <= m <= last]
        if months:
            candidates.append((era, months, r))

    # Multi-month files first, so they claim their months before the monthly files are seen.
    candidates.sort(key=lambda c: (-len(c[1]), c[1][0]))
    taken: set[Month] = set()
    selected = []
    for era, months, r in candidates:
        if any(m in taken for m in months):
            if all(m in taken for m in months):
                continue
            raise ValueError(f"{r['name']!r} partly overlaps files already selected")
        taken.update(months)
        selected.append(
            SourceFile(
                system="dublin",
                kind="readings",
                era=era,
                name=f"readings-{months[0]}" + (f"--{months[-1]}" if len(months) > 1 else ""),
                url=r["url"],
                months=tuple(str(m) for m in months),
            )
        )
    return sorted(selected, key=lambda f: f.months[0])


def dublin_catalog(first: Month, last: Month) -> list[SourceFile]:
    with urllib.request.urlopen(_request(DUBLIN_PACKAGE), timeout=60) as resp:
        package = json.load(resp)["result"]
    if package.get("license_id") != "CC-BY-4.0":
        raise RuntimeError(
            f"Dublinbikes licence is now {package.get('license_id')!r}, not CC BY 4.0"
        )
    return select_dublin(package["resources"], first, last)


def helsinki_catalog(first: Month, last: Month) -> list[SourceFile]:
    """The station register and every trips file that exists in [first, last].

    Seasons have no fixed start (March in 2020, April or May otherwise) and the winter months have
    no file, so a month is in the catalog only if its URL answers.
    """
    files = [SourceFile("helsinki", "stations", "hsl", "stations", HELSINKI_STATIONS, ())]
    for m in months_between(first, last):
        url = HELSINKI_TRIPS.format(year=m.year, month=m.month)
        if _exists(url):
            files.append(SourceFile("helsinki", "trips", "hsl", f"trips-{m}", url, (str(m),)))
    return files


def _request(url: str, method: str = "GET") -> urllib.request.Request:
    return urllib.request.Request(url, method=method, headers={"User-Agent": USER_AGENT})


def _exists(url: str) -> bool:
    try:
        with urllib.request.urlopen(_request(url, "HEAD"), timeout=60) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise
