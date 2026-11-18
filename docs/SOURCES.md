# Sources

Both sources are public, both are CC BY 4.0, and neither carries personal data. Each was read from
its portal's own metadata on 2026-11-17, and a sample of files was profiled before any design
decision rested on it. Every figure below comes from those samples; phase 1 recomputes them over
the full load.

## Dublin: Dublinbikes station state

- **Publisher:** Dublin City Council, dataset `dublinbikes-api` on data.gov.ie (mirrored on
  Smart Dublin).
- **Licence:** CC BY 4.0, set at dataset level.
- **Credit line:** "Contains Dublinbikes data from Dublin City Council, published on data.gov.ie
  under CC BY 4.0."
- **What it is:** the state of every station over time. No trips, no bikes, no users.

The history comes in three file eras, and two different schemas:

| Era | Files | Cadence | Columns |
|---|---|---|---|
| 2018-07 to 2021-12 | quarterly, ~300 MB each | 5 min | `STATION ID, TIME, LAST UPDATED, NAME, BIKE STANDS, AVAILABLE BIKE STANDS, AVAILABLE BIKES, STATUS, ADDRESS, LATITUDE, LONGITUDE` |
| 2021-10 to 2024-02 | monthly, ~17 MB | 30 min | same, with underscores (`BIKE_STANDS`, ...) |
| 2024-05 onwards | monthly, ~80 MB | 5 min | GBFS shaped: `system_id, last_reported, station_id, num_bikes_available, num_docks_available, is_installed, is_renting, is_returning, name, short_name, address, lat, lon, region_id, capacity` |

What the samples showed (2022-06, 2023-10, 2025-03, 2025-10):

- **The two schemas disagree on time.** The older files are in Irish local wall-clock time: on
  2023-10-29, when clocks went back, the 01:00 and 01:30 snapshots appear twice, told apart only by
  a second of jitter (`01:00:02` and `01:00:03`). The GBFS files are on a uniform clock: both
  daylight saving days of 2025 have exactly 288 five-minute slots, which a local clock cannot have.
  They are read as UTC.
- **`TIME` and `LAST UPDATED` are different things.** `TIME` is when the file's producer polled;
  `LAST UPDATED` is when the station last reported. The median gap is 226 s, and in 140 rows of
  2023-10 the station's report is later than the poll that carried it.
- **Bikes plus free docks do not always equal capacity:** 6 to 7% of rows in every sample. Docks
  out of service are the likely cause. The target must not enforce that sum.
- **Station ids are stable across the schema change:** all 114 ids of 2023-10 map to the same name
  in 2025-10, which adds one station.
- **Gaps:** no files for 2024-03 and 2024-04; the portal lists 2024-08 at 21 bytes and some months
  marked as having historical data issues. The listed sizes are stale (2023-10 is listed at 8.3 MB
  and downloads at 18.4 MB), so completeness is measured from content, never from metadata.
- **No duplicate `(station, poll time)` pairs** in any sample.
- **Two eras overlap:** the last quarterly file (2021-10 to 2022-01, 5 min) and the first monthly
  files (2021-10 to 2021-12, 30 min) cover the same three months. The load takes one of them, by a
  rule written down, never both.
- **A file's name is not its content.** The quarterly file named 2021-10-01 to 2022-01-01 also
  carries all of 2022-01-01 (31,968 rows), which the January file carries again. Found by the
  full load, as stations whose reports went back a day at that boundary. Every file is now cut to
  the months it was assigned, on its own clock; this is the only file in the window with rows
  outside them.

## Helsinki and Espoo: city bike trips

- **Publisher:** HSL (Helsinki Region Transport), dataset
  `helsingin-ja-espoon-kaupunkipyorilla-ajatut-matkat` on Helsinki Region Infoshare. The data is
  owned by City Bike Finland.
- **Licence:** CC BY 4.0.
- **Credit line:** "Contains city bike trip data owned by City Bike Finland, published by HSL
  (© HSL 2026) under CC BY 4.0."
- **Files:** `dev.hsl.fi/citybikes/od-trips-<year>/<year>-<mm>.csv`, one per month of the season
  (April or May to October), 2016 onwards. The host answers with a 307 to object storage.
- **Columns:** `Departure, Return, Departure station id, Departure station name, Return station id,
  Return station name, Covered distance (m), Duration (sec.)`. UTF-8 with a byte order mark.

What the samples showed (2016-07, 2025-07, 2025-10):

- **No trip id.** A trip is only its values. 2016-07 has 6 rows that are exact duplicates of
  another; 2025 has none.
- **Local wall-clock time, no zone.** On 2025-10-26, the hour that repeats when Helsinki leaves
  summer time holds 72 departures against 56 the hour before and 23 after.
- **Station id formats changed.** 2016 uses `A04`, `B08`; 2025 uses three digits with leading
  zeros (`006`, `043`). The leading zero is part of the id.
- **Service and test stations ride along:** `997 Workshop Helsinki`, `999 Relay Box test station`,
  `*40 Puotila` (with a leading space in the name), `900`, `902`.
- **Duration is not return minus departure.** Median difference 4 s, but more than 60 s in 1.4% of
  2025-07 and 2.9% of 2025-10, up to hours. Both are kept as published; neither is derived from the
  other.
- **Defects kept on purpose:** 536 null distances and 26 negative distances in 2025-07; 10 trips
  returning before they depart in 2025-10; one departure written as a bare date (`2025-10-01`).
- **What the full load added** (all 70 files, 27,041,535 trips): 209,264 trips with no duration,
  665 with no departure station and 360 with no return station, all from 2017 onwards; and from
  2018, distances in centimetres (`1941.67`) in about 14,000 trips. All of it is kept as published.

### Helsinki and Espoo: stations

- **Publisher:** HSL, dataset `hsl-n-kaupunkipyoraasemat` on Helsinki Region Infoshare, CC BY 4.0.
  Same credit as above.
- **Columns:** trilingual names (`Nimi`, `Namn`, `Name`), addresses in Finnish and Swedish, city,
  operator, capacity (`Kapasiteet`), WGS84 coordinates and a projected pair.
- **347 of the 457 have a city and an operator of one space** (`' '`), not empty and not null.
- **457 stations.** 7 of the 458 departure ids seen in 2025-07 are not in it: the service and test
  stations above, and stations since removed. Trips reference stations the register does not list.

## What this means for the design

The sources are loaded into the legacy systems as they are, defects included, because a migration
that only ever sees clean data proves nothing. Each defect above has a rule in
[MAPPING_SPEC.md](MAPPING_SPEC.md), and the verifier checks the rule, not the absence of the defect.
