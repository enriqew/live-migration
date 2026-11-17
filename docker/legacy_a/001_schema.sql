-- Legacy A: Dublin. Local wall-clock time, station state as readings.
-- The schema is specified in docs/MAPPING_SPEC.md, section 1.

CREATE TABLE station (
    number       integer PRIMARY KEY,
    name         text NOT NULL,
    address      text,
    latitude     numeric(8,6) NOT NULL,
    longitude    numeric(9,6) NOT NULL,
    bike_stands  smallint NOT NULL
);

CREATE TABLE station_reading (
    reading_id     bigserial PRIMARY KEY,
    station_number integer NOT NULL REFERENCES station(number),
    reported_at    timestamp NOT NULL,   -- Europe/Dublin wall clock, no zone
    polled_at      timestamp,            -- Europe/Dublin wall clock; null from 2024-05
    bikes          smallint NOT NULL,
    free_stands    smallint NOT NULL,
    status         text NOT NULL CHECK (status IN ('OPEN', 'CLOSED')),
    is_renting     boolean,              -- added 2024-05, null before
    is_returning   boolean               -- added 2024-05, null before
);

CREATE INDEX station_reading_station_reported ON station_reading (station_number, reported_at);

CREATE TABLE rental (
    rental_id     bigserial PRIMARY KEY,
    from_station  integer NOT NULL REFERENCES station(number),
    to_station    integer REFERENCES station(number),
    started       timestamp NOT NULL,    -- Europe/Dublin wall clock
    ended         timestamp
);
