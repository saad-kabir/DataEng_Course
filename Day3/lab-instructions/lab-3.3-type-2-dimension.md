# Lab 3.3 · A type 2 dimension that holds up

**Time** 50 minutes · **Slide 37** · **Systems** Iceberg (via Trino), MinIO
**SQL** `Day3/labs/work/lab_3_3_scd2.sql`

## Objective

Build `dim_driver` as a type 2 Iceberg table, apply a real change and an irrelevant
one, then show that joining facts to the *current* row silently moves last month's
revenue between cities.

## Before you start

The Iceberg catalog is up and Trino can see it:

```bash
cd DataEng_Course/Day1/labs
curl -s "http://localhost:8181/v1/config?warehouse=s3://rides-lake/iceberg" | head -c 60; echo
docker compose exec trino trino --execute "SHOW CATALOGS;"
```

`iceberg` must be in that list. The catalog holds table pointers; MinIO holds the
data and metadata files.

> The deck's slide 3 adds Iceberg jars to Spark. We use **Trino** instead: it has been
> in the stack since Day 1, its Iceberg connector does everything these two labs need,
> and it is one fewer moving part. The lesson is unchanged — and slide 3's own point,
> that several engines read one copy, is easier to believe when the engine reading the
> table is not the one that wrote it.

## Steps

### 1. Build the fixture (10 min)

```bash
docker compose exec -T trino trino -f /work/day3/lab_3_3_scd2.sql
```

Read the file while it runs. It creates:

- `gold.dim_driver` — three drivers, each with one open version. `valid_to` is
  **exclusive** and `NULL` means current
- `gold.fact_trip` — four trips, two of them for driver 1, either side of a change
- `gold.driver_changes` — two incoming changes

The changes are chosen deliberately. Driver 1 genuinely moves from Manhattan to
Brooklyn on 3 January. Driver 2 only changes a **phone number** — an untracked
column, which must not create a new version.

Note the hash:

```sql
to_hex(md5(to_utf8(city || '|' || licence_class)))
```

**Tracked columns only.** Hashing every column, phone included, would open a new
version for driver 2 and multiply your dimension with rows nobody will ever query.

### 2. Apply the type 2 merge (15 min)

Two statements. Close the old version, then insert the new one:

```bash
docker compose exec -T trino trino -f /dev/stdin <<'SQL'
MERGE INTO iceberg.gold.dim_driver d
USING (
  SELECT driver_id, city, licence_class, phone, changed_at,
         to_hex(md5(to_utf8(city || '|' || licence_class))) AS new_hash
  FROM iceberg.gold.driver_changes
) c
ON d.driver_id = c.driver_id AND d.is_current
WHEN MATCHED AND d.tracked_hash <> c.new_hash
  THEN UPDATE SET valid_to = c.changed_at, is_current = false;

INSERT INTO iceberg.gold.dim_driver
SELECT c.driver_id, c.city, c.licence_class, c.phone,
       to_hex(md5(to_utf8(c.city || '|' || c.licence_class))),
       c.changed_at, NULL, true
FROM iceberg.gold.driver_changes c
WHERE NOT EXISTS (
  SELECT 1 FROM iceberg.gold.dim_driver d
  WHERE d.driver_id = c.driver_id AND d.is_current
);
SQL
```

Then look at what you have:

```bash
docker compose exec trino trino --execute "
SELECT driver_id, city, phone, valid_from, valid_to, is_current
  FROM iceberg.gold.dim_driver ORDER BY driver_id, valid_from;"
```

```
1  Manhattan  555-0001  2024-01-01  2024-01-03  false
1  Brooklyn   555-0001  2024-01-03  <null>      true
2  Brooklyn   555-0002  2024-01-01  <null>      true
3  Queens     555-0003  2024-01-01  <null>      true
```

**Driver 1 has two versions. Driver 2 still has one.** The phone change moved nothing,
because it is not in the hash. That is the whole difference between a dimension you
can query and one that doubles every quarter.

### 3. Join the facts two ways (15 min)

```bash
docker compose exec -T trino trino -f /dev/stdin <<'SQL'
SELECT 'historical (correct)' AS method, d.city, sum(f.revenue) AS revenue
FROM iceberg.gold.fact_trip f
JOIN iceberg.gold.dim_driver d
  ON f.driver_id = d.driver_id
 AND f.event_ts >= d.valid_from
 AND f.event_ts <  coalesce(d.valid_to, TIMESTAMP '9999-12-31 00:00:00')
GROUP BY d.city
UNION ALL
SELECT 'naive current-row', d.city, sum(f.revenue)
FROM iceberg.gold.fact_trip f
JOIN iceberg.gold.dim_driver d
  ON f.driver_id = d.driver_id AND d.is_current
GROUP BY d.city
ORDER BY 1, 2;
SQL
```

```
historical (correct)  Brooklyn   290.0
historical (correct)  Manhattan  100.0
historical (correct)  Queens      60.0
naive current-row     Brooklyn   390.0
naive current-row     Queens      60.0
```

Stop and look at that. **Manhattan is gone.** Not understated — absent from the
report entirely. Its £100 of January revenue is now attributed to Brooklyn, because
driver 1 moved there in a later month.

Nothing errored. Nothing was null. The totals still add to 450. This is the reason
type 2 exists, and it is why slide 26 says to ask the business one question per
attribute: *when this changes, should last quarter's numbers change too?*

### 4. Break the range on purpose (10 min)

Make `valid_to` inclusive instead of half-open and re-run the historical join:

```bash
docker compose exec trino trino --execute "
SELECT count(*) AS fact_rows_matched
FROM iceberg.gold.fact_trip f
JOIN iceberg.gold.dim_driver d
  ON f.driver_id = d.driver_id
 AND f.event_ts >= d.valid_from
 AND f.event_ts <= coalesce(d.valid_to, TIMESTAMP '9999-12-31 00:00:00');"
```

Compare with the half-open version (`<` instead of `<=`). A fact landing exactly on a
boundary instant matches **both** versions and is counted twice. Here the fixture has
no fact at exactly midnight on the 3rd, so the counts agree — insert one and watch it
double.

## Verify

```bash
docker compose exec trino trino --execute "
SELECT driver_id, count(*) AS versions, count(*) FILTER (WHERE is_current) AS current_rows
  FROM iceberg.gold.dim_driver GROUP BY driver_id ORDER BY driver_id;"
```

```
1  2  1
2  1  1
3  1  1
```

Driver 1 has two versions; everyone has **exactly one** current row. If any driver
shows two current rows, the merge and the insert did not land together.

## Record

| Driver | Versions | Why |
|---|---|---|
| 1 | | |
| 2 | | |
| 3 | | |

| Method | Manhattan | Brooklyn | Queens |
|---|---|---|---|
| historical | | | |
| naive current-row | | | |

Then answer in one line each:

- Revenue that moved city under the naive join: **____**
- What would hashing `phone` as well have cost you?
- How would a reader notice the naive join was wrong, without being told?

## Discuss

1. The naive join produced a plausible, non-null, correctly-totalling report. What
   process catches this class of error, given no test failed?
2. Slide 27 says the merge and the insert must be one transaction. What does a reader
   see in the window between them if they are not?
3. Which attributes on a driver dimension would you make type 1, and how would you
   defend that to someone who wants everything tracked?

## Stretch

Add a `valid_to` of `TIMESTAMP '9999-12-31'` instead of `NULL` for current rows and
rewrite both joins without `coalesce`. Then argue for one of the two conventions —
there is a real answer about index usage and about what a null means to a reader.

## What this leaves for lab 3.4

The `iceberg.gold` schema stays. Lab 3.4 works in `iceberg.silver` and does not touch
it.
