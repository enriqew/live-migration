-- Legacy B: Helsinki. Finnish and Swedish column names, station names copied into
-- every trip, no foreign keys. Specified in docs/MAPPING_SPEC.md, section 1.

CREATE TABLE asema (
    id           varchar(8)   NOT NULL PRIMARY KEY,
    nimi         varchar(100) NOT NULL,
    namn         varchar(100),
    name         varchar(100),
    osoite       varchar(200),
    adress       varchar(200),
    kaupunki     varchar(50),
    operaattori  varchar(100),
    kapasiteetti smallint,
    x            decimal(16,13),  -- longitude, every published digit (up to 13 decimals)
    y            decimal(16,13)   -- latitude
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE matka (
    id                      bigint       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    departure               datetime     NOT NULL,
    `return`                datetime     NOT NULL,
    departure_station_id    varchar(8)   NOT NULL,
    departure_station_name  varchar(100),
    return_station_id       varchar(8)   NOT NULL,
    return_station_name     varchar(100),
    covered_distance_m      decimal(10,2),  -- metres; 2018 onwards some carry centimetres
    duration_sec            int          NOT NULL,
    KEY matka_departure (departure)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- The load runs as the application user and needs LOAD DATA LOCAL.
GRANT RELOAD, REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'helsinki'@'%';
