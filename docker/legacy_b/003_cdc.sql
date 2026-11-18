-- Debezium's signal table; see the Dublin twin.
CREATE TABLE debezium_signal (
    id    varchar(42) NOT NULL PRIMARY KEY,
    type  varchar(32) NOT NULL,
    data  varchar(2048)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
