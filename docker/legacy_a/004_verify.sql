-- Checksum markers for the continuous verification (src/verifier/continuous.py).
--
-- The verifier reads one chunk of a table with FOR SHARE and inserts a row here in the same
-- transaction. The row travels through CDC with everything committed before it; the apply
-- service, when it reaches the marker, sets aside the target's rows for that chunk. The two sides
-- are then the same chunk at the same source position, and can be compared while writes go on.
CREATE TABLE verify_marker (
    id            uuid PRIMARY KEY,
    legacy_table  text   NOT NULL,
    lo            bigint NOT NULL,   -- the chunk: keys lo..hi, or the whole table for stations
    hi            bigint NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);
