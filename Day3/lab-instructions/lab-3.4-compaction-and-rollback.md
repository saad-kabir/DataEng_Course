# Lab 3.4 · Small files, compaction, rollback

**Time** 50 minutes · **Slide 38** · **Systems** Iceberg (via Trino), MinIO
**SQL** `Day3/labs/work/lab_3_4_microbatches.sql`

## Objective

Create the small-file problem on purpose, measure what it costs the *planner*, fix it
with one statement, then destroy a partition and undo it by moving a pointer — until
you expire the snapshots and find out that recoverability and retention are the same
setting.

## Before you start

Lab 3.3 is complete, so Trino's `iceberg` catalog works. Nothing from 3.3 is needed
here; this lab lives in `iceberg.silver`.

## Steps

### 1. Make 500 small files (10 min)

```bash
cd DataEng_Course/Day1/labs
docker compose exec -T trino trino -f /work/day3/lab_3_4_microbatches.sql
```

About 80 seconds. It runs 500 separate `INSERT` statements of 100 rows each. Each one
is its own Iceberg **commit**, so each writes its own data file *and* its own manifest.
This is a day of per-minute micro-batches, compressed into a minute and a half.

```bash
docker compose exec -T trino trino -f /dev/stdin <<'SQL'
SELECT count(*) AS data_files, sum(record_count) AS rows,
       cast(avg(file_size_in_bytes) AS bigint) AS avg_bytes
  FROM iceberg.silver."trips_micro$files";
SQL
```

```
 data_files | rows  | avg_bytes
------------+-------+-----------
        500 | 50000 |      1271
```

50,000 rows in 500 files averaging **1.2 kB**. Slide 36 says to target half a
gigabyte per file. You are five orders of magnitude out.

### 2. Measure what it costs (10 min)

Wall time is the wrong instrument here — use `EXPLAIN ANALYZE`, which separates
planning from execution:

```bash
docker compose exec trino trino --execute "
EXPLAIN ANALYZE SELECT pu_zone_id, sum(fare)
  FROM iceberg.silver.trips_micro GROUP BY pu_zone_id;" | grep -i planning
```

```
Queued: 1.03ms, Analysis: 34.93ms, Planning: 80.46ms, Execution: 662.17ms
```

Write both numbers down. **Planning is 80 ms to decide how to read 50,000 rows** — the
engine is opening and reasoning about 500 manifests before it touches any data.

### 3. Compact (5 min)

One statement:

```bash
docker compose exec trino trino --execute \
 "ALTER TABLE iceberg.silver.trips_micro EXECUTE optimize;"
```

> Slide 38 shows Spark's `CALL system.rewrite_data_files(...)`. Trino spells the same
> operation `ALTER TABLE ... EXECUTE optimize`. Same job, same result.

```bash
docker compose exec -T trino trino -f /dev/stdin <<'SQL'
SELECT count(*) AS data_files, cast(avg(file_size_in_bytes) AS bigint) AS avg_bytes
  FROM iceberg.silver."trips_micro$files";
SQL
docker compose exec trino trino --execute "
EXPLAIN ANALYZE SELECT pu_zone_id, sum(fare)
  FROM iceberg.silver.trips_micro GROUP BY pu_zone_id;" | grep -i planning
```

```
 data_files | avg_bytes
------------+-----------
          1 |    137521

Queued: 212.71us, Analysis: 22.38ms, Planning: 7.67ms, Execution: 157.31ms
```

**Planning 80 ms → 7.7 ms. Execution 662 ms → 157 ms.** Not one row changed. Compaction
is a new snapshot, not a mutation — which is exactly why it is safe to run on a live
table while people are querying it.

### 4. Break the table, then undo it (15 min)

Note the good snapshot and the totals first:

```bash
docker compose exec trino trino --execute "
SELECT snapshot_id FROM iceberg.silver.\"trips_micro\$snapshots\"
 ORDER BY committed_at DESC LIMIT 1;"

docker compose exec trino trino --execute "
SELECT count(*) AS rows, round(sum(fare),1) AS total FROM iceberg.silver.trips_micro;"
```

```
 50000 | 1962500.0
```

Now do the damage — a bad run that wipes a day:

```bash
docker compose exec trino trino --execute \
 "DELETE FROM iceberg.silver.trips_micro WHERE dt = DATE '2024-01-05';"

docker compose exec trino trino --execute \
 "SELECT count(*) AS rows FROM iceberg.silver.trips_micro;"
```

```
 48200
```

1,800 rows gone. Roll it back, substituting the snapshot id you noted:

```bash
docker compose exec trino trino --execute \
 "CALL iceberg.system.rollback_to_snapshot('silver','trips_micro',<SNAPSHOT_ID>);"

docker compose exec trino trino --execute \
 "SELECT count(*) AS rows, round(sum(fare),1) AS total FROM iceberg.silver.trips_micro;"
```

```
 50000 | 1962500.0
```

Identical, to the penny, and **instant** — because nothing moved. Rollback swaps one
pointer in the catalog. The data files were never deleted; the newer snapshot simply
stopped referencing them.

### 5. Expire the snapshots, and lose the ability (10 min)

Note the oldest snapshot id, then expire everything:

```bash
docker compose exec trino trino --execute "
SELECT count(*) AS snapshots FROM iceberg.silver.\"trips_micro\$snapshots\";"

docker compose exec trino trino --session iceberg.expire_snapshots_min_retention=0s \
  --execute "ALTER TABLE iceberg.silver.trips_micro
             EXECUTE expire_snapshots(retention_threshold => '0s');"
```

Note that the session property is required. Trino refuses a retention under **7 days**
by default:

```
Retention specified (0.00d) is shorter than the minimum retention configured in the
system (7.00d)
```

That guardrail exists precisely because of what you are about to do. Now:

```bash
docker compose exec trino trino --execute "
SELECT count(*) AS snapshots FROM iceberg.silver.\"trips_micro\$snapshots\";"

docker compose exec trino trino --execute \
 "CALL iceberg.system.rollback_to_snapshot('silver','trips_micro',<OLD_SNAPSHOT_ID>);"
```

```
 1

Query ... failed: Cannot roll back to unknown snapshot id: 7095508699388804663
```

503 snapshots became 1, and the rollback you performed sixty seconds ago is now
impossible. **Retention policy and recoverability are the same setting.** Every hour
of retention you trim off the storage bill is an hour of "undo" you no longer have.

## Verify

```bash
docker compose exec -T trino trino -f /dev/stdin <<'SQL'
SELECT count(*) AS data_files FROM iceberg.silver."trips_micro$files";
SELECT count(*) AS snapshots  FROM iceberg.silver."trips_micro$snapshots";
SELECT count(*) AS rows, round(sum(fare),1) AS total FROM iceberg.silver.trips_micro;
SQL
```

One data file, one snapshot, 50,000 rows totalling 1,962,500.0 — the data survived
everything you did to it.

## Record

| Stage | Data files | Avg file size | Planning | Execution |
|---|---|---|---|---|
| 500 micro-batches | | | | |
| after compaction | | | | |

| Stage | Rows | Snapshots |
|---|---|---|
| before damage | | |
| after DELETE | | |
| after rollback | | |
| after expiry | | |

Then answer in one line each:

- Planning time, before vs after: **____ ×** faster
- How many rows did compaction change? **____**
- Which setting decides how far back you can undo? **____**

## Discuss

1. Compaction cut planning by 10× and execution by 4×. Which of those two would grow
   on a table 1,000 times larger, and why?
2. Rollback was instant because it moves a pointer. What does that imply about what
   `expire_snapshots` actually deletes, and why it is irreversible?
3. Slide 36 lists four maintenance jobs. You have run two. Who owns them on your
   platform today, and what tells you when one has not run?

## Stretch

Re-create the table, take a snapshot id, then run `optimize` **while** a long query is
reading it. Confirm the reader sees a consistent table throughout. That is snapshot
isolation from slide 30, and it is what makes it safe to schedule compaction against a
table people are using.

## What Day 3 leaves for Day 4

The Iceberg tables and the CDC connector stay. Day 4 uses this same Postgres for its
row-level security work.
