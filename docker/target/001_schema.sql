-- The target. Specified in docs/MAPPING_SPEC.md, section 1.
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";   -- uuid_generate_v5, for the verifier's own ids

CREATE SCHEMA fleet;
CREATE SCHEMA migration;

CREATE TABLE fleet.system (
    system_code  text PRIMARY KEY,
    time_zone    text NOT NULL
);
INSERT INTO fleet.system VALUES ('dublin', 'Europe/Dublin'), ('helsinki', 'Europe/Helsinki');

CREATE TABLE fleet.station (
    station_id   uuid PRIMARY KEY,
    system_code  text NOT NULL REFERENCES fleet.system,
    source_ref   text NOT NULL,
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

CREATE TABLE fleet.station_status (
    status_id       uuid PRIMARY KEY,
    station_id      uuid NOT NULL REFERENCES fleet.station,
    reported_at     timestamptz NOT NULL,
    polled_at       timestamptz,
    bikes_available integer NOT NULL CHECK (bikes_available >= 0),
    docks_available integer NOT NULL CHECK (docks_available >= 0),
    is_open         boolean NOT NULL,
    is_renting      boolean,
    is_returning    boolean
);
CREATE INDEX station_status_station_reported ON fleet.station_status (station_id, reported_at);

CREATE TABLE fleet.trip (
    trip_id               uuid PRIMARY KEY,
    system_code           text NOT NULL REFERENCES fleet.system,
    departure_station_id  uuid REFERENCES fleet.station,  -- null only when the source had none
    return_station_id     uuid REFERENCES fleet.station,
    departed_at           timestamptz NOT NULL,
    returned_at           timestamptz,
    duration_s            integer,
    distance_m            numeric(10,2) CHECK (distance_m >= 0),
    flags                 text[] NOT NULL DEFAULT '{}'
);
-- A stub station is removed when no trip names it any more: these answer that question.
CREATE INDEX trip_departure_station ON fleet.trip (departure_station_id);
CREATE INDEX trip_return_station ON fleet.trip (return_station_id);

-- The version guard. One row per target row ever written, deletes included: a delete leaves a
-- tombstone with its position, so an insert of the same row arriving late and out of order is
-- recognised as older and refused instead of resurrecting the row.
-- A source position is (major, minor), compared as a pair:
--   Dublin:   (commit LSN, event LSN)
--   Helsinki: (binlog file number * 2^32 + event position, row within the event)
CREATE TABLE migration.row_version (
    target_table  text   NOT NULL,
    row_id        uuid   NOT NULL,
    source        text   NOT NULL,
    pos_major     bigint NOT NULL,
    pos_minor     bigint NOT NULL,
    deleted       boolean NOT NULL DEFAULT false,
    PRIMARY KEY (target_table, row_id)
);

-- Kafka offsets, committed in the same transaction as the rows they produced.
CREATE TABLE migration.consumer_offset (
    topic       text    NOT NULL,
    partition   integer NOT NULL,
    next_offset bigint  NOT NULL,
    PRIMARY KEY (topic, partition)
);

CREATE TABLE migration.quarantine (
    source       text        NOT NULL,
    legacy_table text        NOT NULL,
    legacy_key   text        NOT NULL,
    rule         text        NOT NULL,
    raw          jsonb       NOT NULL,
    pos_major    bigint      NOT NULL,
    pos_minor    bigint      NOT NULL,
    recorded_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, legacy_table, legacy_key)
);

-- Source transactions already applied. After a restart, events above the committed offsets are
-- read again; one whose transaction is listed here is applied on its own (a no-op under the
-- version guard) instead of waiting for an END that lies below the committed offset of the
-- transaction topic and will never be read again. Pruned once it is older than any replay.
CREATE TABLE migration.applied_txn (
    source      text        NOT NULL,
    txn_id      text        NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, txn_id)
);

-- The unified API, after the cutover. Its idempotency record takes in the legacy APIs' recent
-- keys at the cutover, so a retry of a write that landed on a legacy system is recognised here.
CREATE TABLE migration.api_idempotency (
    key           text PRIMARY KEY,
    request_hash  text NOT NULL,
    response      jsonb,
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- Legacy-compatible keys for rows the unified API creates, so that every target id is still
-- uuid5(<system>:<table>:<legacy key>) and a rollback can write the same row back. Set past each
-- legacy maximum by the cutover script, once the legacy systems are fenced.
CREATE SEQUENCE migration.seq_dublin_rental;
CREATE SEQUENCE migration.seq_dublin_reading;
CREATE SEQUENCE migration.seq_helsinki_matka;

-- The reverse replication, during the rollback window after the cutover.
-- Every row the unified API touches has its legacy key recorded here, in the same transaction:
-- a target id is a hash and cannot be read backwards, and a delete carries only the id.
CREATE TABLE migration.legacy_key (
    row_id        uuid PRIMARY KEY,
    target_table  text NOT NULL,
    system        text NOT NULL,
    legacy_table  text NOT NULL,
    legacy_key    text NOT NULL
);
CREATE TABLE migration.reverse_offset (
    topic       text    NOT NULL,
    partition   integer NOT NULL,
    next_offset bigint  NOT NULL,
    PRIMARY KEY (topic, partition)
);
-- Written by the rollback inside its fence. Captured with the fleet tables, so once it is in Kafka
-- everything the unified API committed before the fence is in Kafka too.
CREATE TABLE migration.rollback_marker (
    id          text PRIMARY KEY,
    written_at  timestamptz NOT NULL DEFAULT now()
);
-- The tables the reverse connector reads. The slot is created at the cutover, inside the fence.
CREATE PUBLICATION reverse_fleet FOR TABLE fleet.station, fleet.station_status, fleet.trip,
    migration.rollback_marker;
