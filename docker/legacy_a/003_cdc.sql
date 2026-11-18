-- Debezium's signal table. Incremental snapshots are requested by inserting a row here, and the
-- snapshot's low and high watermarks are written here too, so the table is captured like the
-- business tables: the watermarks have to travel in the change stream to be of any use.
CREATE TABLE debezium_signal (
    id    varchar(42) PRIMARY KEY,
    type  varchar(32) NOT NULL,
    data  varchar(2048)
);
