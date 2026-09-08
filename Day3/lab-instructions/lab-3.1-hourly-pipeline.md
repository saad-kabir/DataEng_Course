# Lab 3.1 · An hourly pipeline you can re-run

**Time** 50 minutes · **Slide 17** · **Systems** Airflow, Postgres
**DAG** `Day3/labs/work/dags/trips_hourly.py`

## Objective

Load one hour of trips two ways — by append and by merge — then re-run the same
interval and watch only one of them stay correct.

## Before you start

The stack is up. Free some memory first; Day 3 uses neither Flink nor Neo4j:

```bash
cd DataEng_Course/Day1/labs
docker compose stop flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 neo4j
docker compose ps
```

Create the bronze tables:

```bash
docker compose exec -T postgres psql -U de -d rides -f /work/day3/day3_bronze_schema.sql
```

```
 status
---------------------
 bronze schema ready
```

Open Airflow at **http://localhost:8079**. There is no login. It is on 8079 because
Trino has had 8080 since Day 1.

## Steps

### 1. Read the DAG before you run it (10 min)

Open `Day3/labs/work/dags/trips_hourly.py`. Three things earn their comments.

**The timetable.** The DAG does *not* say `schedule="@hourly"`:

```python
HOURLY_INTERVAL = CronDataIntervalTimetable("0 * * * *", timezone="UTC")
```

Airflow 3 made `CronTriggerTimetable` the default for cron strings. That timetable
fires *at* the cron time and sets `data_interval_start == data_interval_end`. Every
window in this DAG would be zero-width, every task would process zero rows, and
**every run would still be green**. Slide 13's whole lesson depends on asking for the
interval timetable by name.

**The interval is an input.** `extract` takes `data_interval_start` and
`data_interval_end` from the context and filters on them. It never calls `now()`.

**The two loads.** `load_append` inserts. `load_merge` does slide 7's MERGE on
`trip_id`. Same source rows, same interval, different write pattern.

### 2. Run one interval (10 min)

Unpause the DAG, then run the 08:00 hour of 15 January 2024 — a window with real data
in it:

```bash
docker compose exec airflow-scheduler airflow dags unpause trips_hourly

docker compose exec airflow-scheduler airflow backfill create \
  --dag-id trips_hourly \
  --from-date 2024-01-15T08:00:00+00:00 \
  --to-date   2024-01-15T08:00:00+00:00 \
  --max-active-runs 2
```

> Slide 14 shows `airflow dags backfill --start-date ... --end-date ...`. That command
> was **removed** in Airflow 3. It is `airflow backfill create` with `--from-date` and
> `--to-date` now.

Watch it in the UI. When it is green:

```bash
docker compose exec postgres psql -U de -d rides -c "
SELECT (SELECT count(*) FROM bronze.trips_staging) AS staging,
       (SELECT count(*) FROM bronze.trips_append)  AS appended,
       (SELECT count(*) FROM bronze.trips_merge)   AS merged;"
```

```
 staging | appended | merged
---------+----------+--------
    2342 |     2342 |   2342
```

Three identical numbers. Both write patterns are correct — **exactly once**.

### 3. Find the data interval in the UI (5 min)

Open the run in the Airflow UI and find its **data interval**. It reads
08:00 → 09:00 while the run itself started today. That one-hour offset is the whole
point of slide 13: the run owns a fixed window of *data*, not a moment in time.

### 4. Re-run the same interval (10 min)

This is the lab.

```bash
docker compose exec airflow-scheduler airflow backfill create \
  --dag-id trips_hourly \
  --from-date 2024-01-15T08:00:00+00:00 \
  --to-date   2024-01-15T08:00:00+00:00 \
  --max-active-runs 2 --reprocess-behavior completed
```

`--reprocess-behavior completed` is what lets a backfill re-run an interval that
already succeeded. Then count again:

```bash
docker compose exec postgres psql -U de -d rides -c "
SELECT (SELECT count(*) FROM bronze.trips_append) AS appended,
       (SELECT count(*) FROM bronze.trips_merge)  AS merged,
       (SELECT count(DISTINCT trip_id) FROM bronze.trips_append) AS distinct_ids;"
```

```
 appended | merged | distinct_ids
----------+--------+--------------
     4684 |   2342 |         2342
```

**The append path doubled.** 4,684 rows for 2,342 distinct trips. The merge path did
not move. And Airflow reported success both times — nothing in the orchestrator knows
or cares that one of these tables is now wrong.

### 5. Backfill three days, bounded (15 min)

```bash
docker compose exec airflow-scheduler airflow backfill create \
  --dag-id trips_hourly \
  --from-date 2024-01-16T00:00:00+00:00 \
  --to-date   2024-01-18T23:00:00+00:00 \
  --max-active-runs 2
```

72 intervals, at most 2 at a time. While it runs, watch what the source database
feels:

```bash
docker compose exec postgres psql -U de -d rides -c \
 "SELECT count(*) AS connections FROM pg_stat_activity WHERE datname='rides';"
```

Slide 14's point in one number: concurrency against your own source is the first
thing that breaks in a backfill, and `max_active_runs` is the dial that bounds it.

## Verify

```bash
docker compose exec postgres psql -U de -d rides -c "
SELECT count(*) AS merged_rows, count(DISTINCT trip_id) AS distinct_ids
  FROM bronze.trips_merge;"
```

`merged_rows` and `distinct_ids` must be **equal**. That equality is the definition of
idempotent, and it survives every re-run you just did.

## Record

| Step | appended | merged | distinct trip_ids |
|---|---|---|---|
| after first run | | | |
| after re-run | | | |
| after 3-day backfill | | | |

Then answer in one line each:

- Ratio of `appended` to `distinct_ids` after the re-run: **____**
- What in Airflow told you the append path was wrong? **____**
- Peak connections to `rides` during the backfill: **____**

## Discuss

1. Both runs were green. Where does a duplicate-row bug actually get caught, and how
   long would it take you to notice on a real platform?
2. `max_active_runs=2` bounded the damage. Name two other resources a 90-day backfill
   at full parallelism would exhaust.
3. The DAG had to ask for `CronDataIntervalTimetable` by name. If you had shipped it
   with `@hourly` on Airflow 3, what would you have seen in the UI, and what would you
   have seen in the data?

## Stretch

Set `retries=0` on `load_merge`, break the SQL deliberately, and re-run. Then fix it
and re-run again. Confirm the table is still correct — that is what idempotency buys
you, and it is why slide 15 says to fix the write pattern before raising the retry
count.

## What this leaves for lab 3.2

Nothing. Lab 3.2 uses its own small source table so Debezium's initial snapshot does
not have to publish two million rows before the interesting part starts.
