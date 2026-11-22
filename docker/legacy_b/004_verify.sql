-- Checksum markers for the continuous verification; see the Dublin twin.
CREATE TABLE verify_marker (
    id            char(36)    NOT NULL PRIMARY KEY,
    legacy_table  varchar(32) NOT NULL,
    lo            bigint      NOT NULL,
    hi            bigint      NOT NULL,
    created_at    datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
