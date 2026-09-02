# Lab 0 · Environment setup

**Time** 30 minutes, ideally the evening before
**Objective** Get the stack running and both datasets on disk, so lab 1.1 starts on a
working machine.

## What you need

- Docker Desktop or Docker Engine with Compose v2 (`docker compose version` prints
  v2.x or later — the old hyphenated `docker-compose` is not used anywhere in these
  labs)
- 8 GB of RAM free. The stack idles at about 2.5 GB; the headroom is for lab 2.2,
  where Spark runs `local[*]` inside the marimo container.
- About 12 GB of disk: roughly 8 GB of images (the marimo image alone is 5 GB,
  because it carries PySpark and a pre-warmed jar cache), plus volumes and Docker's
  build cache.
- [uv](https://docs.astral.sh/uv/) for anything you run outside a container:
  `curl -LsSf https://astral.sh/uv/install.sh | sh`

Every image in the stack has a native `arm64` build, so Apple Silicon Macs run the
whole week without emulation. That matters more than it sounds: labs 1.2 and 2.2 ask
you to record query timings, and an emulated database would give you numbers that
cannot be compared with anyone else's in the room.

## Where everything lives

All paths in every lab sheet are relative to the course root, `DataEng_Course/`.
Every `docker compose` command is run from `DataEng_Course/Day1/labs` — that is the
directory holding `docker-compose.yml`, and Compose resolves all its relative
volume mounts from there.

```
DataEng_Course/
└── Day1/
    ├── lab-instructions/     the lab sheets you are reading
    │   └── instructor/       instructor copy — not in the learner bundle
    └── labs/                 ← run docker compose from here
        ├── docker-compose.yml
        ├── data/             fetch.sh + the taxi Parquet and zone lookup  → /data
        ├── import/           the three OpenFlights .dat files → Neo4j's import dir
        ├── seed/             01_schema.sql, run once on first Postgres start
        ├── trino/catalog/    Trino catalog properties
        ├── work/             notebooks and load scripts → /work in marimo
        └── marimo/           Dockerfile + pyproject.toml for the notebook image
```

## 1. Get the data

```bash
cd DataEng_Course/Day1/labs
chmod +x data/fetch.sh
./data/fetch.sh
```

This downloads about 55 MB: the taxi Parquet and zone lookup into
`Day1/labs/data/`, and the three OpenFlights files into `Day1/labs/import/`, which
is mounted as Neo4j's import directory. `fetch.sh` creates `import/` if it is
missing — do not move the files afterwards.

## 2. Start the services Day 1 needs

Still in `DataEng_Course/Day1/labs`:

```bash
docker compose up -d
docker compose ps
```

That starts five long-running services: `postgres`, `minio`, `trino`, `neo4j`,
`marimo`. Wait until every line reads `healthy` — all five have a healthcheck, so
`docker compose ps` is the only readiness signal you need.

You will also see a sixth container, `minio-init`, if you run `docker compose ps -a`.
It is a one-shot that creates the `rides-lake` bucket and exits. `Exited (0)` is the
correct final state for it, not a failure.

Two services are deliberately *not* started by `up -d`:

- `pg_replica`, behind the `replica` profile — lab 1.1 starts it by name.
- `spark-master` and `spark-worker`, behind the `spark` profile
  (`docker compose --profile spark up -d`). Day 1 does not use them: lab 2.2 runs
  PySpark `local[*]` inside the `marimo` container and writes to MinIO from there.

Day 2's Kafka and Flink services are not in the file yet — they are sketched in a
commented block at the bottom of `docker-compose.yml` and will be added with the
Day 2 labs.

## 3. Load Postgres

```bash
docker compose exec marimo uv run python /work/load_postgres.py --rows 2000000
```

About 90 seconds. `/work` inside the container is `Day1/labs/work/` on your machine,
so the script you are running is `Day1/labs/work/load_postgres.py`. It loads 265
zones and 2 million trips into the empty schema that `Day1/labs/seed/01_schema.sql`
created on first start.

## 4. Create the MinIO bucket

The `minio-init` container already created `rides-lake` when the stack came up.
Run these anyway: they are idempotent, they set up the `local` alias that later labs
use interactively, and they tell you the bucket is really there.

```bash
docker compose exec minio mc alias set local http://localhost:9000 admin password
docker compose exec minio mc mb --ignore-existing local/rides-lake
```

## Verify

Run all four. Each should print what is shown.

```bash
# Postgres has data
docker compose exec postgres psql -U de -d rides -c "SELECT count(*) FROM trips;"
#  count
# ---------
#  2000000

# MinIO bucket exists
docker compose exec minio mc ls local/
# [2026-...]     0B rides-lake/

# Trino answers
docker compose exec trino trino --execute "SELECT count(*) FROM tpch.tiny.orders;"
# "15000"

# Neo4j can see the OpenFlights files
docker compose exec neo4j ls -1 /var/lib/neo4j/import
# airlines.dat
# airports.dat
# routes.dat
```

Open the two web interfaces and leave them open for the day:

| Service | URL | Login |
|---|---|---|
| marimo notebooks | http://localhost:8888 | none |
| MinIO console | http://localhost:9001 | admin / password |
| Trino UI | http://localhost:8080 | any username |
| Neo4j Browser | http://localhost:7474 | neo4j / password |

## Record

| Check | Expected | Yours |
|---|---|---|
| `docker compose ps` healthy services | 5 | |
| Rows in `trips` | 2,000,000 | |
| `Day1/labs/data` size on disk | ~48 MB | |
| Time for the marimo image to build | 2-3 min | |

## If something fails

**Wrong directory** — `no configuration file provided` means you are not in
`DataEng_Course/Day1/labs`. `docker compose` only finds the stack from there; if you
must run it from elsewhere, use `docker compose -f DataEng_Course/Day1/labs/docker-compose.yml`.

**Port already in use** — something local is on 5432, 8080 or 8888. Change the
`published:` value of that port in `docker-compose.yml`, for example `"15432"` under
the Postgres `target: 5432`.

**`load_postgres.py` cannot connect** — Postgres is still initialising. Wait for
`docker compose logs postgres` to print "database system is ready to accept
connections" and run it again. The script is safe to re-run, though it appends, so
truncate first: `TRUNCATE trips RESTART IDENTITY;`

**marimo build fails on the JRE install** — you are behind a proxy that blocks the
Debian mirrors. Run marimo on the host instead:
`cd DataEng_Course/Day1/labs/marimo && uv sync && uv run marimo edit ../work`
