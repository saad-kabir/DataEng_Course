# Advanced Data Engineering - Day 3 labs

Four labs, 50 minutes each, on the same Docker Compose stack as Days 1 and 2.

| # | Lab | System | Time | File |
|---|-----|--------|------|------|
| 3.1 | An hourly pipeline you can re-run | Airflow, Postgres | 50 min | [lab-3.1-hourly-pipeline.md](lab-3.1-hourly-pipeline.md) |
| 3.2 | CDC into bronze, tested | Debezium, Kafka | 50 min | [lab-3.2-cdc-into-bronze.md](lab-3.2-cdc-into-bronze.md) |
| 3.3 | A type 2 dimension that holds up | Iceberg, Trino | 50 min | [lab-3.3-type-2-dimension.md](lab-3.3-type-2-dimension.md) |
| 3.4 | Small files, compaction, rollback | Iceberg, Trino | 50 min | [lab-3.4-compaction-and-rollback.md](lab-3.4-compaction-and-rollback.md) |

## No setup lab, again

From `DataEng_Course/Day1/labs`:

```bash
docker compose up -d
```

That now brings up **fourteen** services: the nine from Days 1 and 2, plus
`airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `kafka-connect`
and `iceberg-rest` (and a one-shot `airflow-init` that exits 0).

**Memory.** The fourteen-service stack idles at about **4.4 GB** — heavier than Day 2's
5.4 GB figure suggests only because Airflow's three containers are lighter at rest than
Flink's. Budget the same **12 GB free** as Day 2: the headroom is for Airflow's task
processes, which the LocalExecutor forks per running task.

Day 3 uses neither Flink nor Neo4j, and stopping them takes idle memory to about
**2.8 GB**:

```bash
docker compose stop flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 neo4j
```

## What Day 3 adds

| Service | Image | Port | Used by |
|---|---|---|---|
| `airflow-apiserver` | `apache/airflow:3.3.1` | **8079** | lab 3.1 |
| `airflow-scheduler` | `apache/airflow:3.3.1` | - | lab 3.1 |
| `airflow-dag-processor` | `apache/airflow:3.3.1` | - | lab 3.1 |
| `kafka-connect` | `quay.io/debezium/connect:3.6.2.Final` | 8083 | lab 3.2 |
| `iceberg-rest` | `apache/iceberg-rest-fixture:1.10.1` | 8181 | labs 3.3, 3.4 |

Airflow keeps its metadata in the **same Postgres** the labs use, in its own
`airflow` database, and runs the LocalExecutor. Postgres now runs
`wal_level=logical` so Debezium can decode its WAL; that is a superset of the
`replica` setting Day 1 needed, so lab 1.1 still works.

The Iceberg REST catalog holds table pointers and MinIO holds the data and metadata
files. **Trino is the engine for both Iceberg labs** - the deck's slide 3 suggests
adding Iceberg jars to Spark, but Trino is already running and its Iceberg connector
does everything labs 3.3 and 3.4 need. One fewer moving part, same lesson.

## Where everything lives

```
DataEng_Course/
├── Day1/labs/                ← run docker compose from here, as on Days 1 and 2
└── Day3/
    ├── lab-instructions/     these lab sheets
    └── labs/work/            Day 3's lab files  → /work/day3
        ├── dags/trips_hourly.py        the Airflow DAG, lab 3.1
        ├── day3_bronze_schema.sql      bronze tables, lab 3.1
        ├── day3_cdc_source.sql         CDC source table, lab 3.2
        ├── day3_debezium_connector.json  connector config, lab 3.2
        ├── lab_3_3_scd2.sql            type 2 fixture, lab 3.3
        └── lab_3_4_microbatches.sql    500 micro-batches, lab 3.4
```

`Day3/labs/work` is mounted at `/work/day3` in postgres and trino, and at
`/opt/airflow/day3` (with `dags/` at `/opt/airflow/dags`) in the Airflow containers.

## The web interfaces you will need today

| Service | URL | Login |
|---|---|---|
| **Airflow** (lab 3.1) | http://localhost:8079 | none - every visitor is an admin |
| Kafka Connect REST (lab 3.2) | http://localhost:8083 | none |
| Iceberg REST catalog | http://localhost:8181 | none |
| MinIO console | http://localhost:9001 | admin / password |

Airflow is on **8079**, not the 8080 the deck shows: Trino has owned 8080 since
Day 1.

## Two places the deck predates the software

Both are called out in the sheets where they bite, and both are worth knowing before
you start.

- **`schedule="@hourly"` no longer gives you a data interval.** Airflow 3 made
  `CronTriggerTimetable` the default for cron strings, and it sets
  `data_interval_start == data_interval_end`. Every window would be zero-width, every
  task would process nothing, and every run would still be green. Lab 3.1's DAG asks
  for `CronDataIntervalTimetable` explicitly.
- **`airflow dags backfill` was removed.** It is `airflow backfill create` in
  Airflow 3, with `--from-date`/`--to-date` rather than `--start-date`/`--end-date`.

## Structure of each lab

The same six sections as Days 1 and 2: Objective, Before you start, Steps, Verify,
Record, Stretch. Instructor notes and worked answers are in
[instructor/](instructor/) - keep that folder out of the learner copy.
