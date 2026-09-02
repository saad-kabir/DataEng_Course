# Lab 1.2 · Indexes and plans

**Time** 50 minutes · **Slide 21** · **System** Postgres

## Objective

Change the *shape* of a query plan, not just its runtime. You will read four plans
against the same 2 million rows and see a sequential scan become an index scan, then
an index-only scan, then watch a fourth index get ignored.

The number that matters here is the plan node, not the millisecond count.

## Before you start

Lab 0 is complete. The `trips` table has 2 million rows and no secondary indexes.
If you promoted the replica in lab 1.1, work against the primary on port 5432.

Open a psql session and keep it for the whole lab:

```bash
cd DataEng_Course/Day1/labs
docker compose exec postgres psql -U de -d rides
```

Turn on timing:

```sql
\timing on
```

## Steps

### 1. Establish the baseline (10 min)

```sql
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT count(*), avg(total_amount)
  FROM trips
 WHERE pu_zone_id = 132
   AND pickup_ts >= '2024-01-08' AND pickup_ts < '2024-01-15';
```

You will see `Parallel Seq Scan on trips` with a `Filter` and a large
`Rows Removed by Filter`. Write down three things: the plan node, the shared buffers
read, and the execution time.

Confirm the table has nothing to use:

```sql
\d trips
SELECT pg_size_pretty(pg_total_relation_size('trips')) AS total,
       pg_size_pretty(pg_indexes_size('trips'))        AS indexes;
```

### 2. Index the wrong column first (10 min)

```sql
CREATE INDEX idx_trips_payment ON trips (payment_type);
ANALYZE trips;

EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT count(*), avg(total_amount)
  FROM trips
 WHERE pu_zone_id = 132
   AND pickup_ts >= '2024-01-08' AND pickup_ts < '2024-01-15';
```

The plan is unchanged. The index exists, is maintained on every write, occupies disk,
and does nothing for this query. Check what it cost you:

```sql
SELECT pg_size_pretty(pg_relation_size('idx_trips_payment'));
```

Now try a query the index *should* help and watch it get skipped anyway:

```sql
EXPLAIN (ANALYZE, COSTS OFF)
SELECT count(*), avg(total_amount) FROM trips WHERE payment_type = 1;
```

`payment_type = 1` matches roughly 70% of rows, so the planner chooses a sequential
scan. Low selectivity beats the presence of an index every time.

`avg(total_amount)` is doing real work in that query. Drop it and ask only for
`count(*)`, and the planner picks an `Index Only Scan` on `idx_trips_payment`
instead: the index covers the whole query, so it never touches the heap and reads
about 1,400 buffers against the 22,000 a sequential scan needs. Low selectivity only
rules out an index when the query still has to visit the heap.

### 3. Index the predicate (10 min)

```sql
CREATE INDEX idx_trips_zone_ts ON trips (pu_zone_id, pickup_ts);
ANALYZE trips;

EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT count(*), avg(total_amount)
  FROM trips
 WHERE pu_zone_id = 132
   AND pickup_ts >= '2024-01-08' AND pickup_ts < '2024-01-15';
```

The node becomes `Bitmap Heap Scan` fed by a `Bitmap Index Scan`, or an
`Index Scan` if the planner estimates few enough rows. Buffers read drops by a large
multiple. That drop is the lab.

### 4. Make it index-only (10 min)

The query still visits the heap to read `total_amount`. Put it in the index:

```sql
CREATE INDEX idx_trips_zone_ts_inc ON trips (pu_zone_id, pickup_ts) INCLUDE (total_amount);
ANALYZE trips;
VACUUM trips;   -- an index-only scan needs the visibility map to be current

EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT count(*), avg(total_amount)
  FROM trips
 WHERE pu_zone_id = 132
   AND pickup_ts >= '2024-01-08' AND pickup_ts < '2024-01-15';
```

Look for `Index Only Scan` and `Heap Fetches: 0`. If heap fetches is not zero, the
`VACUUM` has not caught up. Run it again.

### 5. Break the index with a function (5 min)

```sql
EXPLAIN (ANALYZE, COSTS OFF)
SELECT count(*) FROM trips
 WHERE pu_zone_id = 132 AND date_trunc('day', pickup_ts) = DATE '2024-01-08';
```

Wrapping the column in a function hides it from the index. Compare with the literal
range form:

```sql
EXPLAIN (ANALYZE, COSTS OFF)
SELECT count(*) FROM trips
 WHERE pu_zone_id = 132
   AND pickup_ts >= '2024-01-08' AND pickup_ts < '2024-01-09';
```

### 6. Count the write cost (5 min)

```sql
SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) AS size, idx_scan
  FROM pg_stat_user_indexes WHERE relname = 'trips' ORDER BY 2 DESC;
```

`idx_scan = 0` after a day of traffic is the signal to drop an index. Time the write
penalty:

```sql
\timing on
INSERT INTO trips (pickup_ts, dropoff_ts, pu_zone_id, do_zone_id, total_amount)
SELECT now(), now(), 132, 48, 10.0 FROM generate_series(1, 100000);

DROP INDEX idx_trips_payment, idx_trips_zone_ts, idx_trips_zone_ts_inc;

INSERT INTO trips (pickup_ts, dropoff_ts, pu_zone_id, do_zone_id, total_amount)
SELECT now(), now(), 132, 48, 10.0 FROM generate_series(1, 100000);
```

## Verify

```sql
CREATE INDEX idx_trips_zone_ts_inc ON trips (pu_zone_id, pickup_ts) INCLUDE (total_amount);
VACUUM ANALYZE trips;

EXPLAIN (COSTS OFF)
SELECT count(*), avg(total_amount) FROM trips
 WHERE pu_zone_id = 132 AND pickup_ts >= '2024-01-08' AND pickup_ts < '2024-01-15';
```

Expected — the top scan node reads:

```
Index Only Scan using idx_trips_zone_ts_inc on trips
```

## Record

| Step | Plan node | Buffers read | Time (ms) |
|---|---|---|---|
| 1 · no index | | | |
| 2 · wrong index | | | |
| 3 · matching index | | | |
| 4 · covering index | | | |
| 5 · function on the column | | | |

| Write cost | Rows | Time (ms) |
|---|---|---|
| INSERT 100k with 3 indexes | 100,000 | |
| INSERT 100k with 0 indexes | 100,000 | |

## Discuss

1. Step 2 produced an index that cost disk and write time and returned nothing. How
   would you find one of those in a database you inherited?
2. The covering index removed the heap visit. What did it cost, in bytes and in
   write latency?
3. Step 5 is the single most common performance bug in reporting SQL. Where would
   you catch it: review, linting, or a plan check in CI?

## Stretch

Add `CREATE INDEX idx_trips_brin ON trips USING brin (pickup_ts);` after dropping
the btree, and compare size and plan for a one-month range scan. BRIN is a few
hundred kilobytes against tens of megabytes. Work out what it gives up.

## On GCP

Cloud SQL runs the same planner, so every plan here transfers. BigQuery has no
indexes at all: the equivalents are partitioning and clustering, which is exactly
what lab 2.2 makes you measure.
