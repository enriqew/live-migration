"""The second implementation of docs/MAPPING_SPEC.md: SQL, on DuckDB.

Different engine, different language: this package imports nothing from `migration` (a test
enforces it), and it does not even compute ids with Python's uuid module. Every rule is written
again from the spec:

- ids: UUIDv5 built from SHA-1 in SQL, version and variant bits set by hand;
- time: each distinct wall-clock hour is classified by asking the zone which instants show it
  (two for the repeated autumn hour, none for the spring gap), with no table of transition
  dates; a wall time is then placed by its hour's offsets. The zones here change only on whole
  hours, so the hour decides;
- folds: the Dublin rule as a window over readings in `reading_id` order;
- rounding: DECIMAL `round`, which is half away from zero;
- names: an ASCII-whitespace trim and `nfc_normalize`.

The statements read the legacy copies (`la_*`, `lb_*`) in the local DuckDB file and build one
expected table per target table, with the target's columns, plus the rule a row is quarantined
under when it is.
"""

NAMESPACE_HEX = "c040d936f6555978923651f890c6a707"  # c040d936-f655-5978-9236-51f890c6a707

MACROS = f"""
CREATE OR REPLACE MACRO uuid5(name) AS CAST(
    substr(sha1(unhex('{NAMESPACE_HEX}') || CAST(name AS VARCHAR)::BLOB), 1, 8) || '-' ||
    substr(sha1(unhex('{NAMESPACE_HEX}') || CAST(name AS VARCHAR)::BLOB), 9, 4) || '-5' ||
    substr(sha1(unhex('{NAMESPACE_HEX}') || CAST(name AS VARCHAR)::BLOB), 14, 3) || '-' ||
    substr('89ab', ((instr('0123456789abcdef',
        substr(sha1(unhex('{NAMESPACE_HEX}') || CAST(name AS VARCHAR)::BLOB), 17, 1)) - 1) & 3) + 1,
        1) ||
    substr(sha1(unhex('{NAMESPACE_HEX}') || CAST(name AS VARCHAR)::BLOB), 18, 3) || '-' ||
    substr(sha1(unhex('{NAMESPACE_HEX}') || CAST(name AS VARCHAR)::BLOB), 21, 12) AS UUID);

-- The ASCII whitespace the spec trims, then NFC; empty becomes null.
CREATE OR REPLACE MACRO clean(s) AS
    nullif(nfc_normalize(trim(s, ' ' || chr(9) || chr(10) || chr(13) || chr(12) || chr(11))), '');

-- An instant from a wall time and the offset of the fold it is in.
CREATE OR REPLACE MACRO utc_of(wall, off) AS timezone('UTC', wall - off);
"""

# One row per (zone, wall-clock hour) seen in the data: how many instants show that hour, and the
# UTC offset of the earlier (fold 0) and later (fold 1) of them.
TZ_HOURS = """
CREATE OR REPLACE TABLE tz_hours AS
WITH hours AS (
    SELECT DISTINCT 'Europe/Dublin' AS zone, date_trunc('hour', w) AS h FROM (
        SELECT reported_at AS w FROM la_station_reading UNION ALL
        SELECT polled_at FROM la_station_reading WHERE polled_at IS NOT NULL UNION ALL
        SELECT started FROM la_rental UNION ALL
        SELECT ended FROM la_rental WHERE ended IS NOT NULL)
    UNION
    SELECT DISTINCT 'Europe/Helsinki', date_trunc('hour', w) FROM (
        SELECT departure AS w FROM lb_matka UNION ALL SELECT "return" FROM lb_matka)
),
probe AS (
    -- Instants an hour either side of the one the zone picks; keep those that show this hour.
    SELECT zone, h, c FROM hours,
        unnest([timezone(zone, h) - INTERVAL 1 HOUR, timezone(zone, h),
                timezone(zone, h) + INTERVAL 1 HOUR]) AS u(c)
    WHERE timezone(zone, c) = h
),
instants AS (
    SELECT zone, h, list_sort(list_distinct(list(c))) AS cs FROM probe GROUP BY zone, h
)
SELECT hours.zone, hours.h,
       CASE coalesce(len(cs), 0) WHEN 0 THEN 'nonexistent' WHEN 1 THEN 'single'
            ELSE 'ambiguous' END AS kind,
       hours.h - timezone('UTC', cs[1]) AS off0,
       hours.h - timezone('UTC', cs[-1]) AS off1
FROM hours LEFT JOIN instants USING (zone, h);
"""

DUBLIN = """
CREATE OR REPLACE TABLE exp_station_dublin AS
SELECT uuid5('dublin:station:' || number) AS station_id, 'dublin' AS system_code,
       CAST(number AS VARCHAR) AS source_ref, clean(name) AS name, NULL::VARCHAR AS name_sv,
       NULL::VARCHAR AS name_en, clean(address) AS address, NULL::VARCHAR AS city,
       bike_stands::INTEGER AS capacity, round(latitude, 6)::DECIMAL(8,6) AS latitude,
       round(longitude, 6)::DECIMAL(9,6) AS longitude, 'public' AS kind,
       CASE WHEN clean(name) IS NULL THEN 'blank_name' END AS quarantine_rule,
       CAST(number AS VARCHAR) AS legacy_key
FROM la_station;

-- Within one station and one ambiguous hour, in reading_id order, everything from the first
-- backward step of the wall clock on is fold 1. reported_at and polled_at independently.
CREATE OR REPLACE TABLE dublin_reading_times AS
WITH r AS (
    SELECT s.reading_id, s.station_number, s.reported_at, s.polled_at,
           hr.kind AS rk, hr.off0 AS r_off0, hr.off1 AS r_off1,
           hp.kind AS pk, hp.off0 AS p_off0, hp.off1 AS p_off1
    FROM la_station_reading s
    JOIN tz_hours hr ON hr.zone = 'Europe/Dublin' AND hr.h = date_trunc('hour', s.reported_at)
    LEFT JOIN tz_hours hp ON hp.zone = 'Europe/Dublin' AND hp.h = date_trunc('hour', s.polled_at)
),
steps AS (
    SELECT *,
        CASE WHEN rk = 'ambiguous' THEN coalesce(reported_at < lag(reported_at) OVER (
            PARTITION BY station_number, date_trunc('hour', reported_at), rk
            ORDER BY reading_id), false) ELSE false END AS r_step,
        CASE WHEN pk = 'ambiguous' THEN coalesce(polled_at < lag(polled_at) OVER (
            PARTITION BY station_number, date_trunc('hour', polled_at), pk
            ORDER BY reading_id), false) ELSE false END AS p_step
    FROM r
),
folded AS (
    SELECT *,
        max(r_step::INT) OVER (PARTITION BY station_number, date_trunc('hour', reported_at), rk
            ORDER BY reading_id ROWS UNBOUNDED PRECEDING) AS r_fold,
        max(p_step::INT) OVER (PARTITION BY station_number, date_trunc('hour', polled_at), pk
            ORDER BY reading_id ROWS UNBOUNDED PRECEDING) AS p_fold
    FROM steps
)
SELECT reading_id, rk, pk,
       utc_of(reported_at, CASE WHEN r_fold = 1 THEN r_off1 ELSE r_off0 END) AS reported_utc,
       CASE WHEN polled_at IS NULL THEN NULL
            ELSE utc_of(polled_at, CASE WHEN p_fold = 1 THEN p_off1 ELSE p_off0 END) END
           AS polled_utc
FROM folded;

CREATE OR REPLACE TABLE exp_status AS
SELECT uuid5('dublin:station_reading:' || s.reading_id) AS status_id,
       uuid5('dublin:station:' || s.station_number) AS station_id,
       t.reported_utc AS reported_at, t.polled_utc AS polled_at,
       s.bikes::INTEGER AS bikes_available, s.free_stands::INTEGER AS docks_available,
       s.status = 'OPEN' AS is_open, s.is_renting, s.is_returning,
       CASE WHEN t.rk = 'nonexistent' OR t.pk = 'nonexistent' THEN 'nonexistent_local_time'
            WHEN s.bikes < 0 OR s.free_stands < 0 THEN 'negative_count' END AS quarantine_rule,
       CAST(s.reading_id AS VARCHAR) AS legacy_key
FROM la_station_reading s JOIN dublin_reading_times t USING (reading_id);

CREATE OR REPLACE TABLE exp_trip_dublin AS
WITH r AS (
    SELECT l.*, hs.kind AS sk, hs.off0 AS s_off, coalesce(he.kind, 'single') AS ek, he.off0 AS e_off
    FROM la_rental l
    JOIN tz_hours hs ON hs.zone = 'Europe/Dublin' AND hs.h = date_trunc('hour', l.started)
    LEFT JOIN tz_hours he ON he.zone = 'Europe/Dublin' AND he.h = date_trunc('hour', l.ended)
)
SELECT uuid5('dublin:rental:' || rental_id) AS trip_id, 'dublin' AS system_code,
       uuid5('dublin:station:' || from_station) AS departure_station_id,
       CASE WHEN to_station IS NULL THEN NULL ELSE uuid5('dublin:station:' || to_station) END
           AS return_station_id,
       utc_of(started, s_off) AS departed_at,
       CASE WHEN ended IS NULL THEN NULL ELSE utc_of(ended, e_off) END AS returned_at,
       CASE WHEN ended IS NULL THEN NULL
            ELSE CAST(trunc(epoch(utc_of(ended, e_off)) - epoch(utc_of(started, s_off))) AS INTEGER)
       END AS duration_s,
       NULL::DECIMAL(10,2) AS distance_m,
       list_sort(list_filter([
           CASE WHEN to_station IS NULL THEN 'open_rental' END,
           CASE WHEN ended IS NOT NULL AND utc_of(ended, e_off) < utc_of(started, s_off)
                THEN 'returned_before_departed' END
       ], lambda f: f IS NOT NULL)) AS flags,
       CASE WHEN sk = 'ambiguous' OR ek = 'ambiguous' THEN 'ambiguous_local_time'
            WHEN sk = 'nonexistent' OR ek = 'nonexistent' THEN 'nonexistent_local_time'
       END AS quarantine_rule,
       CAST(rental_id AS VARCHAR) AS legacy_key
FROM r;
"""

HELSINKI = """
CREATE OR REPLACE TABLE exp_trip_helsinki AS
WITH r AS (
    SELECT m.*, hd.kind AS dk, hd.off0 AS d0, hd.off1 AS d1, hr.kind AS rk, hr.off0 AS r0,
           hr.off1 AS r1
    FROM lb_matka m
    JOIN tz_hours hd ON hd.zone = 'Europe/Helsinki' AND hd.h = date_trunc('hour', m.departure)
    JOIN tz_hours hr ON hr.zone = 'Europe/Helsinki' AND hr.h = date_trunc('hour', m."return")
),
combos AS (
    SELECT r.*, fd, fr,
           epoch(utc_of("return", CASE WHEN fr = 1 THEN r1 ELSE r0 END))
             - epoch(utc_of(departure, CASE WHEN fd = 1 THEN d1 ELSE d0 END)) AS elapsed
    FROM r, (VALUES (0), (1)) d(fd), (VALUES (0), (1)) e(fr)
    WHERE (fd = 0 OR r.dk = 'ambiguous') AND (fr = 0 OR r.rk = 'ambiguous')
),
scored AS (
    SELECT *, abs(elapsed - duration_sec) AS miss,
           row_number() OVER (PARTITION BY id ORDER BY abs(elapsed - duration_sec), fd, fr) AS rank,
           min(abs(elapsed - duration_sec)) OVER (PARTITION BY id) AS best
    FROM combos
),
counted AS (
    SELECT *, sum(CASE WHEN miss = best THEN 1 ELSE 0 END) OVER (PARTITION BY id) AS at_best
    FROM scored
),
chosen AS (
    SELECT *,
           -- undecided: an ambiguous trip with no duration, or two fold pairs equally close
           (dk = 'ambiguous' OR rk = 'ambiguous') AND (duration_sec IS NULL OR at_best > 1)
               AS undecided
    FROM counted WHERE rank = 1
),
placed AS (
    SELECT *,
           utc_of(departure, CASE WHEN NOT undecided AND fd = 1 THEN d1 ELSE d0 END) AS dep_utc,
           utc_of("return", CASE WHEN NOT undecided AND fr = 1 THEN r1 ELSE r0 END) AS ret_utc
    FROM chosen
)
SELECT uuid5('helsinki:matka:' || id) AS trip_id, 'helsinki' AS system_code,
       CASE WHEN departure_station_id IS NULL THEN NULL
            ELSE uuid5('helsinki:asema:' || departure_station_id) END AS departure_station_id,
       CASE WHEN return_station_id IS NULL THEN NULL
            ELSE uuid5('helsinki:asema:' || return_station_id) END AS return_station_id,
       dep_utc AS departed_at, ret_utc AS returned_at, duration_sec::INTEGER AS duration_s,
       CASE WHEN covered_distance_m < 0 THEN NULL ELSE covered_distance_m END::DECIMAL(10,2)
           AS distance_m,
       list_sort(list_filter([
           CASE WHEN undecided THEN 'ambiguous_time' END,
           CASE WHEN covered_distance_m < 0 THEN 'negative_distance' END,
           CASE WHEN ret_utc < dep_utc THEN 'returned_before_departed' END,
           CASE WHEN departure_station_id IS NULL THEN 'missing_departure_station' END,
           CASE WHEN return_station_id IS NULL THEN 'missing_return_station' END
       ], lambda f: f IS NOT NULL)) AS flags,
       CASE WHEN dk = 'nonexistent' OR rk = 'nonexistent' THEN 'nonexistent_local_time' END
           AS quarantine_rule,
       CAST(id AS VARCHAR) AS legacy_key,
       departure_station_id AS dep_ref, return_station_id AS ret_ref
FROM placed;

CREATE OR REPLACE TABLE exp_station_helsinki AS
WITH listed AS (
    SELECT uuid5('helsinki:asema:' || id) AS station_id, 'helsinki' AS system_code,
           id AS source_ref, clean(nimi) AS name, clean(namn) AS name_sv, clean(name) AS name_en,
           clean(osoite) AS address, clean(kaupunki) AS city, kapasiteetti::INTEGER AS capacity,
           round(y, 6)::DECIMAL(8,6) AS latitude, round(x, 6)::DECIMAL(9,6) AS longitude,
           CASE WHEN id IN ('900', '902', '997', '999', '*40') THEN 'service' ELSE 'public' END
               AS kind,
           CASE WHEN clean(nimi) IS NULL THEN 'blank_name' END AS quarantine_rule,
           id AS legacy_key
    FROM lb_asema
),
named AS (
    SELECT dep_ref AS ref FROM exp_trip_helsinki
        WHERE quarantine_rule IS NULL AND dep_ref IS NOT NULL
    UNION
    SELECT ret_ref FROM exp_trip_helsinki WHERE quarantine_rule IS NULL AND ret_ref IS NOT NULL
)
SELECT * FROM listed
UNION ALL
SELECT uuid5('helsinki:asema:' || ref), 'helsinki', ref, ref, NULL, NULL, NULL, NULL, NULL,
       NULL, NULL, 'unlisted', NULL, ref
FROM named WHERE ref NOT IN (SELECT id FROM lb_asema);
"""
