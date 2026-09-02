# Lab 2.1 · Object store and planning

**Time** 50 minutes · **Slide 30** · **Systems** MinIO, Trino

## Objective

Treat an object store as what it is: a flat key namespace with no directories, where
listing is an operation with a cost. Then point a query engine at it and watch the
planner read metadata before it reads any data.

## Before you start

Lab 0 is complete and the `rides-lake` bucket exists.

Set up the MinIO client alias once per session:

```bash
cd DataEng_Course/Day1/labs
docker compose exec minio mc alias set local http://localhost:9000 admin password
```

## Steps

### 1. Prove there are no directories (10 min)

```bash
docker compose exec minio mc cp /data/../data/taxi_zone_lookup.csv local/rides-lake/a/b/c/zones.csv 2>/dev/null \
  || docker compose exec -T minio sh -c 'echo "id,name" > /tmp/z.csv && mc cp /tmp/z.csv local/rides-lake/a/b/c/zones.csv'

# the "directories" a and b hold nothing
docker compose exec minio mc ls local/rides-lake/a/
docker compose exec minio mc ls --recursive local/rides-lake/
```

Delete the object and watch every level of the path disappear with it:

```bash
docker compose exec minio mc rm local/rides-lake/a/b/c/zones.csv
docker compose exec minio mc ls --recursive local/rides-lake/
# empty
```

The slashes are part of the key. `a/b/c/` never existed.

### 2. Upload the taxi data and read the metadata (10 min)

```bash
docker compose exec minio mc cp /data/yellow_tripdata_2024-01.parquet \
  local/rides-lake/raw/yellow_tripdata_2024-01.parquet 2>/dev/null \
  || docker cp data/yellow_tripdata_2024-01.parquet \
       $(docker compose ps -q minio):/tmp/t.parquet \
     && docker compose exec minio mc cp /tmp/t.parquet local/rides-lake/raw/yellow_tripdata_2024-01.parquet

docker compose exec minio mc stat local/rides-lake/raw/yellow_tripdata_2024-01.parquet
```

Note the ETag and the size. Now overwrite it with itself and look again:

```bash
docker compose exec minio mc cp local/rides-lake/raw/yellow_tripdata_2024-01.parquet \
  local/rides-lake/raw/yellow_tripdata_2024-01.parquet
docker compose exec minio mc stat local/rides-lake/raw/yellow_tripdata_2024-01.parquet
```

The object was replaced whole. There is no partial write and no append.

### 3. Measure the cost of listing (10 min)

Create many small objects, then time a recursive list:

```bash
docker compose exec -T minio sh -c '
  mkdir -p /tmp/many && cd /tmp/many
  for i in $(seq 1 2000); do echo "row,$i" > part-$i.csv; done
  time mc cp --recursive /tmp/many/ local/rides-lake/many/
'

docker compose exec -T minio sh -c 'time mc ls --recursive local/rides-lake/many/ | wc -l'
docker compose exec -T minio sh -c 'time mc ls --recursive local/rides-lake/raw/  | wc -l'
```

Same bucket, same API, two thousand keys against one. The listing time difference is
the small file problem before a query engine is even involved.

### 4. Point Trino at the bucket (15 min)

```bash
docker compose exec trino trino
```

```sql
CREATE SCHEMA IF NOT EXISTS hive.lake
WITH (location = 's3://rides-lake/warehouse');

CREATE TABLE hive.lake.trips_raw (
  vendorid              bigint,
  tpep_pickup_datetime  timestamp(6),
  tpep_dropoff_datetime timestamp(6),
  passenger_count       double,
  trip_distance         double,
  pulocationid          bigint,
  dolocationid          bigint,
  payment_type          bigint,
  fare_amount           double,
  tip_amount            double,
  total_amount          double
)
WITH (
  external_location = 's3://rides-lake/raw',
  format = 'PARQUET'
);

SELECT count(*) FROM hive.lake.trips_raw;
```

Now read the plan:

```sql
EXPLAIN ANALYZE
SELECT pulocationid, count(*) AS trips, avg(total_amount) AS avg_fare
  FROM hive.lake.trips_raw
 WHERE pulocationid = 132
 GROUP BY pulocationid;
```

Look for the input rows and physical input bytes on the `TableScan`. The whole file
was read, because nothing in the layout let the planner skip anything. Compare against
a query that touches two columns:

```sql
EXPLAIN ANALYZE
SELECT count(*) FROM hive.lake.trips_raw WHERE pulocationid = 132;
```

Column pruning is the one saving available on an unpartitioned file, and Parquet gives
it to you for free.

### 5. Clean up the small objects (5 min)

```bash
docker compose exec minio mc rm --recursive --force local/rides-lake/many/
```

## Verify

```bash
docker compose exec trino trino --execute \
  "SELECT count(*) FROM hive.lake.trips_raw;"
```

Expected:

```
"2964624"
```

(NYC TLC yellow taxi, January 2024. If your count differs, TLC has republished the
month — note your figure and use it consistently for the rest of the day.)

## Record

| Measurement | Yours |
|---|---|
| Time to upload 2,000 small objects | |
| Time to list 2,000 objects | |
| Time to list 1 object | |
| Physical input bytes, `SELECT ... GROUP BY` on 3 columns | |
| Physical input bytes, `SELECT count(*) WHERE pulocationid = 132` | |
| Ratio between the two | |

## Discuss

1. There are no directories, yet every lakehouse layout is built on path prefixes.
   What does that imply about renaming a "folder" of 100,000 objects?
2. The planner read the same bytes for a filtered query as for an unfiltered one.
   What would have to be true of the layout for it to read fewer?
3. Listing 2,000 objects was measurably slower than listing 1. Where does that cost
   land in a query that scans a year of hourly partitions?

## Stretch

Enable versioning on the bucket
(`mc version enable local/rides-lake`), overwrite the Parquet, then
`mc ls --versions local/rides-lake/raw/`. Work out what that does to your storage
bill and to a deletion request.

## On GCP

Cloud Storage behaves identically: flat namespace, prefixes rather than directories,
listing charged per operation in Class A. Everything in this lab transfers by
swapping `s3://` for `gs://` and using the interoperability endpoint.
