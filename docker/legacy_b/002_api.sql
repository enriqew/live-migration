-- The legacy API's idempotency record; see the Dublin twin. Not captured by CDC.
CREATE TABLE api_idempotency (
    `key`         varchar(64)  NOT NULL PRIMARY KEY,
    request_hash  char(64)     NOT NULL,
    response      json,
    created_at    timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
