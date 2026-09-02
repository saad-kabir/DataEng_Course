import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Lab 2.2 - Layout experiment

    Write the same 3M taxi trips four ways, then query all four identically.

    | # | Layout | Path |
    |---|---|---|
    | A | unpartitioned | `trips_flat` |
    | B | partitioned by `dt` | `trips_dt` |
    | C | partitioned by `dt`, sorted by `pu_zone_id` | `trips_sorted` |
    | D | partitioned by `pu_zone_id` | `trips_zone` |

    Run the cells in order. Each write prints elapsed time, object count and size.
    Layout D is slow on purpose - let it finish.
    """)
    return


@app.cell
def _():
    import time
    from pyspark.sql import SparkSession, functions as F

    spark = (
        SparkSession.builder.appName("lab-2.2-layout")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.29.52")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # A 256 KB row-group target. At the default 128 MB every daily file here
        # (~1.6 MB) is a SINGLE row group whose pu_zone_id min/max spans 1-265, so
        # no engine can skip anything and layout C cannot beat layout B. Applied to
        # all four layouts, so B vs C differs only by the sort.
        .config("spark.hadoop.parquet.block.size", 262144)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark
    return F, spark, time


@app.cell
def _(F, spark):
    raw = spark.read.parquet("/data/yellow_tripdata_2024-01.parquet")

    trips = (
        raw.select(
            F.col("tpep_pickup_datetime").alias("pickup_ts"),
            F.col("tpep_dropoff_datetime").alias("dropoff_ts"),
            F.col("PULocationID").cast("int").alias("pu_zone_id"),
            F.col("DOLocationID").cast("int").alias("do_zone_id"),
            F.col("passenger_count").cast("int").alias("passengers"),
            F.col("trip_distance").alias("distance_mi"),
            F.col("fare_amount"),
            F.col("tip_amount"),
            F.col("total_amount"),
            F.col("payment_type").cast("int"),
        )
        .withColumn("dt", F.to_date("pickup_ts"))
        # January 2024 only - the TLC file carries a handful of stray timestamps
        .filter((F.col("dt") >= "2024-01-01") & (F.col("dt") < "2024-02-01"))
        .cache()
    )

    print(f"{trips.count():,} rows, {len(trips.columns)} columns")
    trips.printSchema()
    return (trips,)


@app.cell
def _(mo):
    mo.md("""
    ## Helper: write, then measure what landed
    """)
    return


@app.cell
def _(time):
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
    )

    def measure(prefix):
        n, total = 0, 0
        token = None
        while True:
            kw = {"Bucket": "rides-lake", "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw)
            for o in page.get("Contents", []):
                if not o["Key"].endswith("_SUCCESS"):
                    n += 1
                    total += o["Size"]
            if not page.get("IsTruncated"):
                break
            token = page["NextContinuationToken"]
        return n, total

    results = {}

    def timed_write(name, prefix, writer):
        t0 = time.time()
        writer()
        elapsed = time.time() - t0
        n, total = measure(prefix)
        results[name] = {
            "write_s": round(elapsed, 1),
            "objects": n,
            "mb": round(total / 1024 / 1024, 1),
            "avg_kb": round(total / max(n, 1) / 1024, 1),
        }
        print(f"{name}: {elapsed:.1f}s  {n} objects  {total/1024/1024:.1f} MB  avg {total/max(n,1)/1024:.0f} KB")
        return results[name]

    return results, timed_write


@app.cell
def _(mo):
    mo.md("""
    ## A - unpartitioned
    """)
    return


@app.cell
def _(timed_write, trips):
    timed_write(
        "A flat",
        "trips_flat/",
        lambda: trips.write.mode("overwrite").parquet("s3a://rides-lake/trips_flat"),
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## B - partitioned by dt
    """)
    return


@app.cell
def _(timed_write, trips):
    timed_write(
        "B by dt",
        "trips_dt/",
        lambda: (
            trips.repartition("dt")
            .write.mode("overwrite")
            .partitionBy("dt")
            .parquet("s3a://rides-lake/trips_dt")
        ),
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## C - partitioned by dt, sorted by pu_zone_id

    The only difference from B is `sortWithinPartitions`. Watch what it does to
    stored size (almost nothing) and to bytes read later (a lot).
    """)
    return


@app.cell
def _(timed_write, trips):
    timed_write(
        "C sorted",
        "trips_sorted/",
        lambda: (
            trips.repartition("dt")
            # "dt" MUST come first. write.partitionBy("dt") re-clusters rows on dt,
            # and that sort discards a plain sortWithinPartitions("pu_zone_id") -
            # every row group then reports pu_zone_id 1-265 and C reads exactly what
            # B reads. Naming dt first leaves pu_zone_id as the order WITHIN each day.
            .sortWithinPartitions("dt", "pu_zone_id")
            .write.mode("overwrite")
            .partitionBy("dt")
            .parquet("s3a://rides-lake/trips_sorted")
        ),
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## D - partitioned by pu_zone_id

    265 zones, so 265 directories. This is the anti-pattern. Expect it to take
    several times as long as B and to produce many small objects.
    """)
    return


@app.cell
def _(timed_write, trips):
    timed_write(
        "D by zone",
        "trips_zone/",
        lambda: (
            trips.repartition("pu_zone_id")
            .write.mode("overwrite")
            .partitionBy("pu_zone_id")
            .parquet("s3a://rides-lake/trips_zone")
        ),
    )
    return


@app.cell
def _(mo, results):
    import pandas as pd

    table = pd.DataFrame(results).T
    mo.vstack([mo.md("## What landed"), mo.ui.table(table.reset_index(names="layout"))])
    return


@app.cell
def _(mo):
    mo.md("""
    ## Register the tables in Trino

    Run this in a terminal, then move to the SQL section of the lab sheet:

    ```bash
    docker compose exec trino trino -f /dev/stdin <<'SQL'
    CREATE SCHEMA IF NOT EXISTS hive.lake WITH (location = 's3://rides-lake/warehouse');

    CREATE TABLE IF NOT EXISTS hive.lake.trips_flat (
      pickup_ts timestamp(6), dropoff_ts timestamp(6), pu_zone_id integer,
      do_zone_id integer, passengers integer, distance_mi double, fare_amount double,
      tip_amount double, total_amount double, payment_type integer, dt date
    ) WITH (external_location = 's3://rides-lake/trips_flat', format = 'PARQUET');

    CREATE TABLE IF NOT EXISTS hive.lake.trips_dt (
      pickup_ts timestamp(6), dropoff_ts timestamp(6), pu_zone_id integer,
      do_zone_id integer, passengers integer, distance_mi double, fare_amount double,
      tip_amount double, total_amount double, payment_type integer, dt date
    ) WITH (external_location = 's3://rides-lake/trips_dt', format = 'PARQUET',
            partitioned_by = ARRAY['dt']);

    CREATE TABLE IF NOT EXISTS hive.lake.trips_sorted (
      pickup_ts timestamp(6), dropoff_ts timestamp(6), pu_zone_id integer,
      do_zone_id integer, passengers integer, distance_mi double, fare_amount double,
      tip_amount double, total_amount double, payment_type integer, dt date
    ) WITH (external_location = 's3://rides-lake/trips_sorted', format = 'PARQUET',
            partitioned_by = ARRAY['dt']);

    -- pu_zone_id is the partition column here, so it must be declared LAST.
    CREATE TABLE IF NOT EXISTS hive.lake.trips_zone (
      pickup_ts timestamp(6), dropoff_ts timestamp(6), do_zone_id integer,
      passengers integer, distance_mi double, fare_amount double, tip_amount double,
      total_amount double, payment_type integer, dt date, pu_zone_id integer
    ) WITH (external_location = 's3://rides-lake/trips_zone', format = 'PARQUET',
            partitioned_by = ARRAY['pu_zone_id']);
    SQL
    ```

    Partitioned external tables start with no partitions registered, so the
    `sync_partition_metadata` calls in the lab sheet are what make Trino see the
    directories Spark wrote. Run them after this.

    Then compare `EXPLAIN ANALYZE` on the same filtered query against each layout
    and fill in the Record table in `lab-2.2-layout-experiment.md`.
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
