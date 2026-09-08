# Running Day 3

## Schedule

| Time | Block |
|---|---|
| 09:00 | Slides 1-16, chapter 1 |
| 10:00 | **Lab 3.1** an hourly pipeline you can re-run |
| 10:50 | Break |
| 11:05 | Slides 18-19, delivery |
| 11:30 | **Lab 3.2** CDC into bronze, tested |
| 12:20 | Lunch |
| 13:20 | Slides 22-30, chapter 2 |
| 14:10 | **Lab 3.3** a type 2 dimension that holds up |
| 15:00 | Break |
| 15:15 | Slides 31-36, formats and operations |
| 16:00 | **Lab 3.4** small files, compaction, rollback |
| 16:50 | Close, slide 40 |

## Say this in the first five minutes

1. **Fourteen services, and it still fits in 12 GB free.** Measured idle is 4.4 GB,
   dropping to 2.8 GB once you run
   `docker compose stop flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 neo4j`.
   Do that at the start; nothing today uses either.
2. **Airflow is on http://localhost:8079**, not 8080. Trino has owned 8080 since Day 1.
   There is no login.
3. **Postgres now runs `wal_level=logical`.** It is a superset of `replica`, so
   nothing from Day 1 breaks.

## Four places the deck predates the software

All four are called out in the sheets. Know them before a hand goes up.

- **`schedule="@hourly"` no longer produces a data interval.** Airflow 3 made
  `CronTriggerTimetable` the default for cron strings; it sets
  `data_interval_start == data_interval_end`. The DAG would run, go green, and process
  nothing. Lab 3.1's DAG asks for `CronDataIntervalTimetable` explicitly, and the
  comment in the file explains why. **This is the single most valuable thing in the
  day** — it is a silent-wrong-answer bug in current Airflow.
- **`airflow dags backfill` was removed.** Slide 14's command is Airflow 2. It is
  `airflow backfill create --from-date ... --to-date ...` now.
- **Slide 3 puts Iceberg on Spark.** We use Trino, which has been in the stack since
  Day 1 and whose Iceberg connector does everything labs 3.3 and 3.4 need. Trino
  spells compaction `ALTER TABLE ... EXECUTE optimize` rather than Spark's
  `CALL system.rewrite_data_files`.
- **Slide 3 lists `apache/airflow:2.10` and `tabulario/iceberg-rest`.** The stack runs
  Airflow 3.3.1 and `apache/iceberg-rest-fixture:1.10.1`.

## Demo live rather than making them run it

- **Lab 3.1 step 4, the re-run.** Put the two row counts on the projector side by
  side. 4,684 against 2,342 with 2,342 distinct ids lands better as a single image
  than as something everyone types.
- **Lab 3.3 step 3, the two joins.** Manhattan vanishing from the report is the most
  memorable thing in the day. Read the output aloud.
- **Lab 3.2 step 5, the 102 MB.** Worth running yourself first so the number is
  already on screen; generating it takes a minute of `UPDATE`.

## Failure modes that eat time

| Symptom | Cause | Fix |
|---|---|---|
| DAG green, zero rows processed | cron-string schedule, no data interval | use `CronDataIntervalTimetable` |
| Every task fails, DAG parses fine | `AIRFLOW__API_AUTH__JWT_SECRET` differs between containers | it is set in compose; do not override |
| `httpx.ConnectError` in task logs | `EXECUTION_API_SERVER_URL` pointing at localhost | set in compose to the apiserver hostname |
| `airflow dags backfill` not found | Airflow 3 | `airflow backfill create` |
| Debezium connector RUNNING, topic empty | stale Connect offsets under the same connector name | stop → DELETE offsets → delete → re-register, in that order |
| Connector will not start | `wal_level` is `replica` | compose sets `logical`; restart postgres |
| `fare` is base64 in the change event | `decimal.handling.mode` unset | it is `double` in the shipped config |
| `expire_snapshots` refused | 7-day minimum retention | `--session iceberg.expire_snapshots_min_retention=0s` |
| Lab 3.4 planning times look identical | ran the query once, cold | run it twice and read the second |

## Cut order when behind

1. **Lab 3.1 step 5** (the three-day backfill). Steps 1-4 carry the lesson.
2. **Lab 3.2 step 4** (the silver merge). Steps 1-3 and 5 are the CDC content.
3. **Lab 3.3 step 4** (the inclusive-range break). State the result instead.
4. **Lab 3.4 step 5** (expiry). Demo it; it is 90 seconds on the projector.

**Never cut lab 3.1 step 4 or lab 3.3 step 3.** They are the two moments where a
correct-looking pipeline is shown to be wrong, which is the whole thesis of the day.

## What Day 3 leaves for Day 4

Iceberg tables in `iceberg.gold` and `iceberg.silver`, the CDC connector, and Airflow.
Day 4 reuses this Postgres for row-level security.
