# live-migration

Two bike share systems that model the same business differently, merged into one database while
both keep taking writes.

Work in progress: phase 0 (sources and the mapping spec). Nothing runs yet.

- [docs/SOURCES.md](docs/SOURCES.md): the two public datasets, their licences, and what profiling
  them showed.
- [docs/MAPPING_SPEC.md](docs/MAPPING_SPEC.md): the legacy schemas, the target schema, and every
  rule between them.

## Data

The base data is public. The writes made during a migration run are generated, by a seeded load
generator.

- Contains Dublinbikes data from Dublin City Council, published on data.gov.ie under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- Contains city bike trip data owned by City Bike Finland and city bike station data from HSL,
  both published by HSL (© HSL 2026) under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
