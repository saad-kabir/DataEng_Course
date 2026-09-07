# Advanced Data Engineering - Day 2 labs

Five labs, 50 minutes each, on the same Docker Compose stack as Day 1. Every command
is given. Work through them in order.

| # | Lab | System | Time | File |
|---|-----|--------|------|------|
| 2.1 | Read the plan, then let AQE re-plan it | Spark | 50 min | [lab-2.1-plans-and-aqe.md](lab-2.1-plans-and-aqe.md) |
| 2.2 | Fix a skewed join | Spark | 50 min | [lab-2.2-skewed-join.md](lab-2.2-skewed-join.md) |
| 2.3 | Partitions, groups, offsets | Kafka | 50 min | [lab-2.3-partitions-groups-offsets.md](lab-2.3-partitions-groups-offsets.md) |
| 2.4 | Windows, lateness, recovery | Flink, Kafka | 50 min | [lab-2.4-windows-lateness-recovery.md](lab-2.4-windows-lateness-recovery.md) |
| 2.5 | Point in polygon, three ways | PostGIS | 50 min | [lab-2.5-point-in-polygon.md](lab-2.5-point-in-polygon.md) |

## There is no Day 2 setup lab

Everything Day 2 needs starts with the command you already used yesterday, run from
`DataEng_Course/Day1/labs`:

```bash
docker compose up -d
```

That now brings up nine services: yesterday's five, plus `kafka`,
`flink-jobmanager` and two TaskManagers. Wait until `docker compose ps` shows every
line `healthy`.

The one thing to check, if you were not here for Day 1:

```bash
cd DataEng_Course/Day1/labs
./data/fetch.sh
```

`fetch.sh` is idempotent - running it again when the files are already there costs a
minute and changes nothing. It now also downloads the taxi **zone polygons** that
lab 2.5 needs, which Day 1 did not use.

**Memory.** The Day 2 stack is heavier than Day 1's: budget **12 GB of RAM free**,
not 8. Kafka and the three Flink containers add roughly 3 GB at idle. If your
machine is tight, stop what the current lab does not need - Neo4j and Trino are
idle all of Day 2:

```bash
docker compose stop neo4j trino
```

## What Day 2 adds to the stack

| Service | Image | Ports | Used by |
|---|---|---|---|
| `kafka` | `apache/kafka:4.3.1` | 29092 | labs 2.3, 2.4 |
| `flink-jobmanager` | built from `./flink` | 8081 | lab 2.4 |
| `flink-taskmanager-1` / `-2` | built from `./flink` | - | lab 2.4 |

Kafka runs in **KRaft** mode, so there is no ZooKeeper. The two TaskManagers have
two slots each, four in total - lab 2.4 uses all four on purpose, so that killing a
TaskManager has a visible consequence.

The Flink image is built rather than pulled because the SQL client needs the Kafka
connector jar on its classpath, and that jar is version-locked to the Flink minor
release. See `Day1/labs/flink/Dockerfile`.

## Where everything lives

Paths are relative to the course root, `DataEng_Course/`. Every `docker compose`
command runs from `DataEng_Course/Day1/labs` - Day 2 extends that one stack rather
than adding a second.

```
DataEng_Course/
├── Day1/labs/                ← run docker compose from here, as on Day 1
│   ├── docker-compose.yml    nine services
│   ├── flink/Dockerfile      Flink + the Kafka SQL connector
│   ├── data/                 taxi Parquet, zone lookup, zone polygons, trip_events.jsonl
│   └── work/                 Day 1's notebooks  → /work
└── Day2/
    ├── lab-instructions/     these lab sheets
    └── labs/work/            Day 2's lab files  → /work/day2
        ├── day2_common.py           shared dataset builders for labs 2.1 and 2.2
        ├── day2_make_events.py      builds the trip_events stream
        ├── lab_2_1_aqe.py           notebook, lab 2.1
        ├── lab_2_2_skew.py          notebook, lab 2.2
        ├── lab_2_4_windows.sql      Flink SQL, lab 2.4
        └── day2_spatial_setup.sql   point generation, lab 2.5
```

Day 2's lab files live under `Day2/`, but the **stack does not move** — there is one
Compose file and it stays in `Day1/labs/`. Compose mounts `Day2/labs/work` into the
containers at **`/work/day2`**, so in marimo you will see a `day2/` folder, and every
container path in these sheets starts `/work/day2/`.

## The web interfaces you will need today

| Service | URL | Login |
|---|---|---|
| marimo notebooks | http://localhost:8888 | none |
| **Spark UI** (labs 2.1, 2.2) | http://localhost:4040 | none, only while a notebook holds a session |
| **Flink dashboard** (lab 2.4) | http://localhost:8081 | none |

## Datasets

Day 2 reuses Day 1's **NYC TLC yellow taxi, January 2024** extract throughout. The
taxi data has no driver and no city column, so labs 2.1 and 2.2 derive them: `city`
is the real borough of the pickup zone, `driver_id` is a deterministic hash. See the
docstring in `Day2/labs/work/day2/day2_common.py`.

Lab 2.5 adds the **NYC TLC taxi zone shapefile** - 263 real zone polygons. The trip
extract has carried no latitude or longitude since 2016, so lab 2.5 generates pickup
points *inside* the real polygons, weighted by each zone's real pickup count. That
gives the spatial join both realistic skew and a known correct answer to check
against.

## Structure of each lab

Same six sections as Day 1:

1. **Objective** - one sentence, and the slide it belongs to
2. **Before you start** - what must already be running
3. **Steps** - numbered, every command given verbatim
4. **Verify** - a command and the output you should see
5. **Record** - a table to fill in, which is what you discuss afterwards
6. **Stretch** - for anyone who finishes early

Instructor notes and worked answers are in [instructor/](instructor/) - keep that
folder out of the learner copy.

## A note on lab numbering

The Day 2 deck labels its chapter 3 labs `Lab 3.1` and `Lab 3.2`. That collides with
Day 3, whose labs are also 3.1 to 3.4. These sheets use the scheme that holds across
the whole week - **day.number** - so the deck's `Lab 3.1` (point in polygon) is
**lab 2.5** here. The deck will be re-exported to match.

The deck's `Lab 3.2`, the Sedona exact-versus-H3 join, is **not** in this bundle.
