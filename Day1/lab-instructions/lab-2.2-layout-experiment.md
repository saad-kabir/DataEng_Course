# Lab 2.2 · Layout experiment

**Time** 50 minutes · **Slide 39** · **Systems** Spark (in marimo), Trino, MinIO
**Notebook** `Day1/labs/work/lab_2_2_layout.py`

## Objective

Write the same 3 million rows four ways and query all four identically. The stored
bytes barely move. The bytes *read* move by more than an order of magnitude. This is
the highest-value lab of the day.

## Before you start

Labs 0 and 2.1 are complete. `s3://rides-lake/raw/` holds the taxi Parquet.

Open marimo at **http://localhost:8888** and open `lab_2_2_layout.py`. The notebook
carries every cell; this file is the reading and the record sheet.

## The four layouts

| # | Layout | Written to |
|---|---|---|
| A | unpartitioned, one directory | `s3a://rides-lake/trips_flat` |
| B | partitioned by `dt` | `s3a://rides-lake/trips_dt` |
| C | partitioned by `dt`, sorted by `pu_zone_id` within each partition | `s3a://rides-lake/trips_sorted` |
| D | partitioned by `pu_zone_id` (265 values) | `s3a://rides-lake/trips_zone` |

## Steps

### 1. Run the notebook through layout D (25 min)

Work down the notebook. Each write cell prints elapsed time, object count and total
size. Layout D is deliberately slow — that is the lesson, so let it finish.

The key cell for layout C:

```python
(trips
 .repartition("dt")
 .sortWithinPartitions("pu_zone_id")
 .write.mode("overwrite")
 .partitionBy("dt")
 .parquet("s3a://rides-lake/trips_sorted"))
```

### 2. Register all four in Trino (10 min)

```bash
cd DataEng_Course/Day1/labs
docker compose exec trino trino -f /dev/stdin <<'SQL'
CALL hive.system.sync_partition_metadata('lake', 'trips_dt', 'FULL');
CALL hive.system.sync_partition_metadata('lake', 'trips_sorted', 'FULL');
CALL hive.system.sync_partition_metadata('lake', 'trips_zone', 'FULL');
SQL
```

The notebook creates the table definitions; this call makes Trino aware of the
partition directories Spark wrote.

### 3. Run the same query against each (10 min)

One city, one week:

```sql
EXPLAIN ANALYZE
SELECT count(*), avg(total_amount)
  FROM hive.lake.trips_flat
 WHERE pu_zone_id = 132
   AND dt >= DATE '2024-01-08' AND dt < DATE '2024-01-15';
```

Repeat with `trips_dt`, `trips_sorted`, `trips_zone`. For each, record from the
`TableScan` node: **physical input bytes**, **input rows**, and the number of
**splits**.

### 4. Read the difference (5 min)

- **A to B** — partition pruning removed directories from the plan before any read
- **B to C** — same partitions, fewer bytes, because row-group statistics on
  `pu_zone_id` became selective once the data was sorted
- **C stored size vs B** — nearly identical. Sorting changed the read cost without
  changing what is stored
- **D** — 265 directories, thousands of tiny objects, and a planner that spends longer
  listing than the query spends scanning

## Verify

```bash
docker compose exec minio mc ls --recursive local/rides-lake/ | \
  awk '{split($NF,a,"/"); print a[1]}' | sort | uniq -c
```

Expected, approximately:

```
      1 raw
     31 trips_dt        (one directory per day, a handful of files each)
      4 trips_flat
     31 trips_sorted
    265 trips_zone      (one per zone, most of them tiny)
```

## Record

| Layout | Objects | Stored size | Splits | Input rows | Physical bytes | Query time |
|---|---|---|---|---|---|---|
| A flat | | | | | | |
| B by dt | | | | | | |
| C by dt, sorted | | | | | | |
| D by zone | | | | | | |

Then answer in one line each:

- Bytes read, A vs C: **____ ×** less
- Stored bytes, B vs C: **____ %** different
- Which layout would you ship, and what query pattern would change your mind?

## Discuss

1. Layout C reads a fraction of the bytes and stores the same amount. What did the
   sort actually cost, and who pays it?
2. Layout D is a reasonable choice for exactly one access pattern. Which one, and how
   would you know it was the dominant one before committing?
3. The dashboards your team runs filter on three columns. You can partition on one and
   sort on one. How do you decide?

## Stretch

Write a fifth layout partitioned by `dt` **and** `payment_type`, then run the same
query. Two-level partitioning multiplies directory count. Measure where it helps and
where it becomes layout D.

## What this leaves for Day 2

Leave `s3a://rides-lake/trips_sorted` in place. Day 2's streaming labs use it as the
historical side of the batch-and-stream comparison.

## On GCP

The BigQuery equivalent of layouts B and C is one table:

```sql
CREATE TABLE rides.trips
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY pulocationid
AS SELECT * FROM rides.trips_raw;
```

Run the same filtered query and read `totalBytesProcessed` from the job statistics
for each variant. The ratio should track what you measured in Trino.
