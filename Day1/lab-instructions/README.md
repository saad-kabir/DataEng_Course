# Advanced Data Engineering - Day 1 labs

Six labs, 50 minutes each, on one Docker Compose stack. Every command is given.
Work through them in order: each one assumes the previous one ran.

| # | Lab | System | Time | File |
|---|-----|--------|------|------|
| 0 | Environment setup | all | 30 min | [lab-0-setup.md](lab-0-setup.md) |
| 1.1 | Replication and failover | Postgres | 50 min | [lab-1.1-replication-and-failover.md](lab-1.1-replication-and-failover.md) |
| 1.2 | Indexes and plans | Postgres | 50 min | [lab-1.2-indexes-and-plans.md](lab-1.2-indexes-and-plans.md) |
| 2.1 | Object store and planning | MinIO, Trino | 50 min | [lab-2.1-object-store-and-planning.md](lab-2.1-object-store-and-planning.md) |
| 2.2 | Layout experiment | Spark, Trino | 50 min | [lab-2.2-layout-experiment.md](lab-2.2-layout-experiment.md) |
| 3.1 | Model and traverse | Neo4j | 50 min | [lab-3.1-model-and-traverse.md](lab-3.1-model-and-traverse.md) |
| 3.2 | Bulk load and project | Neo4j, GDS | 50 min | [lab-3.2-bulk-load-and-project.md](lab-3.2-bulk-load-and-project.md) |

## Datasets

**NYC TLC yellow taxi, January 2024** — about 3.0 million trips, 48 MB of Parquet.
Public domain. Used by labs 1.1 through 2.2.

**OpenFlights** — 7,700 airports, 6,100 airlines, 67,000 routes. Open Database
License. Used by labs 3.1 and 3.2.

Both are downloaded by `Day1/labs/data/fetch.sh`, run from `Day1/labs`. Neither
needs an account.

## Where everything lives

Paths in the lab sheets are relative to the course root, `DataEng_Course/`. Every
`docker compose` command is run from `DataEng_Course/Day1/labs`.

```
DataEng_Course/
└── Day1/
    ├── lab-instructions/     these lab sheets (instructor/ is the instructor copy)
    └── labs/                 ← run docker compose from here
        ├── docker-compose.yml
        ├── data/             fetch.sh, taxi Parquet, zone lookup   → /data
        ├── import/           OpenFlights .dat files → Neo4j import dir
        ├── seed/             01_schema.sql
        ├── trino/catalog/    Trino catalog properties
        ├── work/             notebooks and load scripts            → /work
        └── marimo/           Dockerfile + pyproject.toml
```

The stack is Compose v2 (Compose Specification, no `version:` key): `docker compose`,
never `docker-compose`. `docker compose up -d` starts the five Day 1 services, plus a
one-shot `minio-init` that creates the bucket and exits. Behind profiles, and not
started by default: `pg_replica` (`replica`, started by name in lab 1.1) and
`spark-master`/`spark-worker` (`spark`, unused on Day 1 — lab 2.2 runs PySpark
`local[*]` inside marimo). Day 2's Kafka and Flink are not in the file yet.

## Structure of each lab

Every lab file has the same six sections:

1. **Objective** — one sentence, and the slide it belongs to
2. **Before you start** — what must already be running
3. **Steps** — numbered, every command given verbatim
4. **Verify** — a command and the output you should see
5. **Record** — a table to fill in, which is what you discuss afterwards
6. **Stretch** — for anyone who finishes early

Instructor notes and worked answers are in [instructor/](instructor/) — keep that
folder out of the learner copy.

## What Day 1 leaves behind

Lab 2.2 writes four Parquet layouts to `s3a://rides-lake/`. Day 2's streaming labs
read the partitioned-and-sorted variant as their historical side, so leave the MinIO
volume in place at the end of the day.
