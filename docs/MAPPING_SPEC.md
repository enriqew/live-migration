# Mapping spec

The single statement of what the target must contain, given the two legacy systems. It is written
before either implementation, and it is implemented twice: by the apply service (Python), and by
the verifier (SQL), which never imports the apply service. When the two disagree, this document
decides which one is wrong; when this document is silent, both stop.

Status: phase 0 draft, 2026-11-17. Every rule cites the profiling finding it answers
([SOURCES.md](SOURCES.md)).

---

## 1. The three systems

### Legacy A: Dublin (PostgreSQL)

Built the way the older Dublinbikes files look: local wall-clock time, station state as readings.

```sql
CREATE TABLE station (
    number       integer PRIMARY KEY,
    name         text NOT NULL,
    address      text,
    latitude     numeric(8,6) NOT NULL,
    longitude    numeric(9,6) NOT NULL,
    bike_stands  smallint NOT NULL              -- current capacity only, no history
);

CREATE TABLE station_reading (
    reading_id     bigserial PRIMARY KEY,       -- insertion order, monotonic in real time
    station_number integer NOT NULL REFERENCES station(number),
    reported_at    timestamp NOT NULL,          -- WITHOUT time zone: Europe/Dublin wall clock
    polled_at      timestamp,                   -- WITHOUT time zone; null from 2024-05
    bikes          smallint NOT NULL,
    free_stands    smallint NOT NULL,
    status         text NOT NULL,               -- 'OPEN' | 'CLOSED'
    is_renting     boolean,                     -- added 2024-05, null before
    is_returning   boolean                      -- added 2024-05, null before
);

CREATE TABLE rental (                           -- live writes only, the generator's
    rental_id     bigserial PRIMARY KEY,
    from_station  integer NOT NULL REFERENCES station(number),
    to_station    integer REFERENCES station(number),   -- null while the bike is out
    started       timestamp NOT NULL,           -- Europe/Dublin wall clock
    ended         timestamp
);
```

The GBFS-era files are UTC. The loader writes them as Dublin wall-clock time, the convention the
table already has, which means every autumn hour that repeats is ambiguous in legacy A, as it was
in the source for the older files.

### Legacy B: Helsinki (MySQL 8.4, `utf8mb4`, `utf8mb4_0900_ai_ci`)

Built the way HSL publishes: Finnish and Swedish column names, station names copied into every
trip, no foreign keys.

```sql
CREATE TABLE asema (                            -- 'station'
    id           varchar(8) PRIMARY KEY,        -- '043', 'A04', '*40': text, leading zeros kept
    nimi         varchar(100) NOT NULL,         -- Finnish name
    namn         varchar(100),                  -- Swedish name
    name         varchar(100),                  -- English name
    osoite       varchar(200),
    adress       varchar(200),
    kaupunki     varchar(50),
    operaattori  varchar(100),
    kapasiteetti smallint,
    x            decimal(16,13),                -- longitude, as published (up to 13 decimals)
    y            decimal(16,13)                 -- latitude
);

CREATE TABLE matka (                            -- 'trip'
    id                      bigint AUTO_INCREMENT PRIMARY KEY,
    departure               datetime NOT NULL,  -- Europe/Helsinki wall clock
    `return`                datetime NOT NULL,  -- reserved word, quoted on purpose
    departure_station_id    varchar(8) NOT NULL,
    departure_station_name  varchar(100),
    return_station_id       varchar(8) NOT NULL,
    return_station_name     varchar(100),
    covered_distance_m      int,                -- null and negative values exist
    duration_sec            int NOT NULL
);
```

The collation is accent and case insensitive, the MySQL default. Two values it calls equal can be
different strings in PostgreSQL. Nothing in the target relies on MySQL's notion of equality.

### Target (PostgreSQL 18, schema `fleet`)

```sql
CREATE TABLE system (
    system_code  text PRIMARY KEY,              -- 'dublin' | 'helsinki'
    time_zone    text NOT NULL                  -- 'Europe/Dublin' | 'Europe/Helsinki'
);

CREATE TABLE station (
    station_id   uuid PRIMARY KEY,
    system_code  text NOT NULL REFERENCES system,
    source_ref   text NOT NULL,                 -- the legacy key, verbatim
    name         text NOT NULL,
    name_sv      text,
    name_en      text,
    address      text,
    city         text,
    capacity     integer CHECK (capacity >= 0),
    latitude     numeric(8,6),
    longitude    numeric(9,6),
    kind         text NOT NULL CHECK (kind IN ('public', 'service', 'unlisted')),
    UNIQUE (system_code, source_ref)
);

CREATE TABLE station_status (
    status_id       uuid PRIMARY KEY,
    station_id      uuid NOT NULL REFERENCES station,
    reported_at     timestamptz NOT NULL,
    polled_at       timestamptz,
    bikes_available integer NOT NULL CHECK (bikes_available >= 0),
    docks_available integer NOT NULL CHECK (docks_available >= 0),
    is_open         boolean NOT NULL,
    is_renting      boolean,
    is_returning    boolean
);
CREATE INDEX ON station_status (station_id, reported_at);

CREATE TABLE trip (
    trip_id                 uuid PRIMARY KEY,
    system_code             text NOT NULL REFERENCES system,
    departure_station_id    uuid NOT NULL REFERENCES station,
    return_station_id       uuid REFERENCES station,      -- null while a Dublin rental is open
    departed_at             timestamptz NOT NULL,
    returned_at             timestamptz,
    duration_s              integer,
    distance_m              integer CHECK (distance_m >= 0),
    flags                   text[] NOT NULL DEFAULT '{}'
);
```

Plus, in schema `migration`, the bookkeeping the apply service owns: `row_version` (the source
position that last wrote each target row, and a tombstone once it is deleted), `consumer_offset`,
`quarantine`. The verifier reads `quarantine` and nothing else there.

There is no id map. Every target id is a pure function of its legacy key (section 2), so nothing
about identity has to be stored, and the verifier derives the same ids without reading anything
the apply service wrote.

## 2. Identity

- **Every target id is a UUIDv5** of `"<system>:<table>:<legacy key>"` under one fixed namespace
  UUID, recorded in this document at phase 1. The same legacy row always maps to the same target
  row, across restarts, replays and both implementations (`uuid.uuid5` in Python,
  `uuid_generate_v5` in SQL).
- **Legacy keys:** `station.number` (A), `asema.id` (B, the exact string),
  `station_reading.reading_id` (A), `rental.rental_id` (A), `matka.id` (B).
- **Every target row comes from exactly one legacy row.** `station_status` is keyed by the reading,
  not by `(station, reported_at)`: in the older Dublin files two polls can carry the same station
  report, and a natural key would fold two legacy rows into one target row. A delete of one of them
  would then have to know whether the other still exists, and the apply step would stop being a
  function of a single change event. Many-to-one mappings are kept out of the target on purpose.
- **Helsinki has no trip id in its files.** Legacy B assigns `matka.id` at load, in file order, so
  the 6 exact duplicate rows of 2016-07 stay 12 distinct trips. Whether they are real repeats or a
  publishing error cannot be known, and the target does not guess.

## 3. Time

All target timestamps are `timestamptz`. The conversion is the part of this spec most likely to be
wrong, so it is spelled out case by case.

| Case | Rule |
|---|---|
| Legacy wall-clock time that exists once | Convert with the system's zone (`Europe/Dublin`, `Europe/Helsinki`) |
| **Ambiguous, legacy A** (autumn hour repeated) | Per station, in `reading_id` order, the wall clock steps backwards once inside that hour: readings before the step are summer time (`fold=0`), readings after it winter time (`fold=1`). `reading_id` is the only witness of real order, which is why legacy A keeps it. A station with no backward step in the hour gets `fold=0`, and the run report counts those stations |
| **Ambiguous, legacy B** | Of the four `(fold departure, fold return)` combinations, pick the one whose `returned_at - departed_at` is closest to `duration_sec`. On a tie, `fold=0` and the flag `ambiguous_time` |
| Nonexistent (spring gap) | Never seen in the samples. If one appears: quarantined with reason `nonexistent_local_time`, never shifted |
| `polled_at` (A, older era) | Same rules as `reported_at`, independently |

## 4. Values

| Target column | Dublin | Helsinki |
|---|---|---|
| `station.name` | `station.name`, verbatim | `asema.nimi` |
| `station.name_sv`, `name_en` | null | `asema.namn`, `asema.name` |
| all names | Trimmed of leading and trailing whitespace, Unicode NFC. Nothing else: no case change, no prettifying | same |
| `station.capacity` | `bike_stands` | `kapasiteetti` |
| `station.latitude`, `longitude` | `latitude`, `longitude` | `y`, `x` (the projected pair in the source file is not loaded) |
| coordinate rounding | Dublin publishes at most 6 decimals, so nothing is rounded | Rounded to 6 decimals (about 11 cm), **half away from zero**, from the decimal value, never through a float. Python's `Decimal` rounds half to even by default and PostgreSQL's `round(numeric)` half away from zero: the two implementations must agree, and a tie is exactly where they would not |
| `station.kind` | `public` | `service` for the ids listed in section 6; `unlisted` for an id that trips reference and `asema` does not have; `public` otherwise |
| `station_status.is_open` | `status = 'OPEN'` | n/a |
| `station_status` bikes and docks | `bikes`, `free_stands`. Their sum is not checked against capacity (6 to 7% of rows differ) | n/a |
| `trip.distance_m` | null (rentals carry no distance) | `covered_distance_m`; a negative value becomes null with flag `negative_distance` |
| `trip.duration_s` | `ended - started` in seconds, null while open | `duration_sec`, verbatim, never recomputed |
| `trip.flags` | `open_rental` while `to_station` is null | `returned_before_departed` when it does; `ambiguous_time` (section 3); `negative_distance` |

**Stations a trip references and the register lacks** are created by the first trip that names
them, with `kind = 'unlisted'`, the name the trip carries (trimmed, NFC), and every other column
null. A later row in `asema` with the same id upgrades it in place.

## 5. Quarantine

A row that breaks a rule this spec cannot resolve goes to `migration.quarantine` with the legacy
key, the rule and the raw row, and is not written to `fleet`. The verifier's expected target is
"everything not quarantined", and it recomputes the quarantine set on its own: a row quarantined
by one implementation and not the other is a mismatch like any other.

## 6. Lists the rules depend on

Fixed at phase 0, from the samples; phase 1 re-derives them from the full load and this section is
updated with a dated note if they change.

- Helsinki service stations: `900`, `902`, `997`, `999`, `*40`.

## 7. Canonical form, for checksums

Both sides of a comparison are reduced to the same text before hashing:

- Fields in column order, separated by `0x1F`, rows ending in `0x1E`; null as `\N`.
- `timestamptz` as UTC, `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
- `numeric` at the column's declared scale, no exponent, `.` as separator.
- `boolean` as `t` / `f`; `uuid` lowercase, hyphenated; arrays sorted, `{a,b}`.
- Text as UTF-8 NFC bytes, never collated.
- Row hash: SHA-256 of the canonical row. Chunk hash: SHA-256 of the row hashes in primary key order.

## 8. Versions

Pinned 2026-11-18. Images are pinned by digest in `docker-compose.yml`; the tags below are for
reading. Debezium's tag is republished nightly on a new base image, so only the digest is stable.

| Component | Version | Note |
|---|---|---|
| PostgreSQL (legacy A, target) | 18.7 | |
| MySQL (legacy B) | 8.4.13 LTS | |
| Apache Kafka, KRaft | 4.3.2 | The latest patch of the line Debezium's Connect runtime is built on (4.3.1); 4.4.0 was available and not taken |
| Debezium Connect | 3.7.1.Final | The 3.6 line found on 2026-11-17 was already superseded |
| Apicurio Registry | 3.3.4 | KafkaSQL storage |
| Python (APIs, in their image) | 3.13 | Locally 3.14; the project requires 3.13 or later |
