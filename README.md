# live-migration

Two bike share systems that model the same business differently, merged into one database while
both keep taking writes. Dublin runs on PostgreSQL, Helsinki on MySQL; the target is a third
schema neither of them has. About 49 million rows move with change data capture, the clients
switch to the new system in a pause of seconds, and they can switch back the same way.

No claim about the result rests on the migration's own word. An independent verifier, written
without importing a line of the migration, rebuilds the expected target from the legacy systems and
compares it with what is there, row by row and column by column.

- [docs/SOURCES.md](docs/SOURCES.md): the two public datasets, their licences, and what profiling
  them showed.
- [docs/MAPPING_SPEC.md](docs/MAPPING_SPEC.md): the legacy schemas, the target schema, and every
  rule between them.

## How it works

```
 generator ──► gateway ──► legacy API A ──► legacy A (PostgreSQL 18) ─┐
 (open loop)     │                                                    │ Debezium 3.7
                 │       ──► legacy API B ──► legacy B (MySQL 8.4)  ──┤ Kafka 4.3 (KRaft)
                 │                                                    │ Avro, Apicurio 3.3
                 └─ after cutover ─► unified API ──► target (PostgreSQL 18) ◄── apply service
                                                         │
                                                         └── reverse replication ──► legacy A, B
```

**Initial copy.** The connectors start with `snapshot.mode=no_data` and copy every table by
incremental snapshot (the DBLog watermark algorithm), triggered through a signal table. The legacy
systems keep taking writes throughout; a row changed while its chunk is being read is resolved by
the watermarks, not by a lock.

**Apply.** One service per source consumes the change topics and writes the target.

- A source transaction is applied whole or not at all, grouped by Debezium's transaction metadata.
- The consumer offsets are stored in the target, in the same transaction as the rows they produced.
  A crash at any point resumes exactly where the target stands.
- Every row carries its source version in `migration.row_version`, deletes leave a tombstone, and a
  change older than what is stored is dropped. Replays, duplicates and reordering are harmless.
- Batches go in by `COPY` into a staging table and one set-based upsert.
- A column the spec does not know stops the service (fail closed) until it is mapped.

**Cutover.** One command, measured step by step:

1. gate on the replication lag;
2. fence: the gateway holds new requests (it does not refuse them) and waits for those in flight;
3. a marker written in each source's signal table, and the apply services drained to it;
4. the forward connectors paused, the reverse replication slot created on the target, the legacy
   write gate closed;
5. keys and sequences handed over, so the unified API continues each legacy id sequence;
6. the gateway routed to the unified API and released;
7. the reverse connector registered, under a name no connector has had before.

**Rollback** checks its preconditions before it fences anything (traffic on the unified API, the
reverse connector running and having applied something), then runs the same shape backwards: drain
the reverse path, stop it and delete its offsets, hand the keys back, resume the forward
connectors, open the legacy write gate, route, release.

**Verification** runs in DuckDB. It copies the legacy systems and the target, implements the spec
again in SQL (UUIDv5 built from SHA-1, wall-clock hours classified by asking the time zone), and
compares in three levels: counts per bucket, a hash per bucket, then the rows and columns of any
bucket that differs. A test fails the build if the verifier imports the migration.

**Continuous verification** checks the same spec chunk by chunk while both systems keep taking
writes. A source and a target are only comparable at the same source position, so the verifier
reads a chunk with `FOR SHARE` and inserts a checksum marker in the same source transaction. The
marker travels through CDC; the apply service applies it only once every transaction that committed
before it is in the target, holds back the ones that came after, and copies the target's rows for
that chunk aside in the marker's own transaction. The verifier compares that copy with what its spec
derives from the rows it read. A chunk that differs is checked again with a new marker, and is a
finding only if it differs twice. This is the `pt-table-checksum` idea carried over to a CDC
pipeline.

## Results

From the full-size runs on one workstation, every service in Docker Compose:

| | |
|---|---|
| Load from the locked files | 18 minutes |
| Incremental snapshot of 49.4 million rows, with the generator writing | 90 minutes |
| Cutover pause under 50 operations a second | 1.49 s (75 requests held at most) |
| Rollback pause | 2.15 s (108 held) |
| Operations failed in the cycle | 0 of 45,000, none retried, p99 latency 103 ms |
| Frozen verification after the cycle | 49,404,189 rows, all equal |
| Continuous verification under 50 writes a second | 49,400,822 rows in 5,150 chunks in 39 minutes, all equal; locks held 124 ms at the median |

## Chaos suite

Each scenario starts from a fresh environment and declares beforehand what it expects to see.

| # | Scenario |
|---|---|
| 01 | Rows updated and deleted while the incremental snapshot is reading them |
| 03 | Both apply services killed three times mid-snapshot, with writes going on |
| 04 | The Helsinki connector rewound 30 seconds |
| 05 | The last 2,000 events of each topic delivered again, newest first |
| 06 | A column added to a source table mid-run: the service stops, then resumes once it is mapped |
| 07 | The Dublin connector down while writes go on: the slot retains WAL and the alert fires |
| 08 | The target restarted under the apply services mid-snapshot |
| 09 | Cutover and rollback under load: no write fails, each pause under 5 s, nothing lost |
| 10 | A unit bug planted in the transform: the verifier names those rows and that column only |

## Running it

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
docker compose up -d
uv sync

uv run python -m loader fetch --config conf/dev.toml    # files pinned by SHA-256 in the lock
uv run python -m loader load --config conf/dev.toml
uv run python -m loader.cdc register
uv run python -m loader.cdc snapshot
uv run python -m migration.apply dublin                 # and helsinki
uv run python -m generator.run --rate 20 --seconds 600

uv run python -m cutover.run
uv run python -m migration.reverse
uv run python -m cutover.rollback
uv run python -m verifier.run
uv run python -m verifier.continuous                    # while the generator writes

uv run python -m chaos.run                              # the suite, or name scenarios
uv run python -m chaos.full_run                         # the full-size run, end to end
uv run pytest
```

`conf/dev.toml` is a few months of each city; `conf/load.toml` is the full window (Dublin
2021-10 to 2026-06, Helsinki 2016 to 2025, 5.1 GB of source files).

## Data

The base data is public. The writes made during a migration run are generated, by a seeded load
generator.

- Contains Dublinbikes data from Dublin City Council, published on data.gov.ie under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- Contains city bike trip data owned by City Bike Finland and city bike station data from HSL,
  both published by HSL (© HSL 2026) under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

## License

The code is MIT licensed (see [LICENSE](LICENSE)). The data keeps its own licences, listed above.
