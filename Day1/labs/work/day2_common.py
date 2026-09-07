"""Shared dataset builders for the Day 2 Spark labs (2.1 and 2.2).

Day 2 deliberately reads the raw January 2024 taxi Parquet that `data/fetch.sh`
already downloaded, rather than the lake tables lab 1.4 writes. That keeps Day 2
runnable after nothing more than `docker compose up -d`, which is the point: there
is no Day 2 setup lab.

The taxi extract has no driver and no city, so both are derived here:

  city       real, from the zone lookup borough of the pickup zone
  driver_id  synthetic but deterministic - a hash of the pickup zone and minute,
             so the same row always lands on the same driver and every run of
             every lab produces the same numbers.
"""

from pyspark.sql import SparkSession, functions as F

TAXI_PARQUET = "/data/yellow_tripdata_2024-01.parquet"
ZONE_LOOKUP = "/data/taxi_zone_lookup.csv"

N_DRIVERS = 8000


def spark_session(app_name, **conf):
    """A local[*] session. Day 2's Spark labs need no object store, so unlike
    Day 1's lab 1.4 there are no S3A jars to resolve and startup is a few seconds."""
    b = SparkSession.builder.appName(app_name)
    for k, v in conf.items():
        b = b.config(k.replace("__", "."), v)
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    return s


def zones(spark):
    """zone_id -> borough, from the lookup CSV. 265 rows, broadcast-sized."""
    return (
        spark.read.option("header", True).csv(ZONE_LOOKUP)
        .select(
            F.col("LocationID").cast("int").alias("zone_id"),
            F.col("Borough").alias("city"),
        )
        # 'Unknown' and 'N/A' boroughs are noise in a city aggregate.
        .where(~F.col("city").isin("Unknown", "N/A"))
    )


def trips(spark, limit_rows=None):
    """Taxi trips with a real city and a synthetic driver_id.

    Returns columns: trip_id, driver_id, city, fare, pickup_ts.
    """
    df = spark.read.parquet(TAXI_PARQUET)
    if limit_rows:
        df = df.limit(limit_rows)
    df = (
        df.select(
            F.col("PULocationID").cast("int").alias("zone_id"),
            F.col("tpep_pickup_datetime").alias("pickup_ts"),
            F.col("total_amount").cast("double").alias("fare"),
        )
        .where(F.col("fare").between(0, 500))
        .withColumn(
            # pmod keeps it non-negative; hash() is deterministic across runs.
            "driver_id",
            F.pmod(F.hash("zone_id", F.minute("pickup_ts")), F.lit(N_DRIVERS)),
        )
        .withColumn("trip_id", F.monotonically_increasing_id())
    )
    return (
        df.join(zones(spark), "zone_id")
        .select("trip_id", "driver_id", "city", "fare", "pickup_ts")
    )


def drivers(spark):
    """A small driver dimension, one row per driver_id. About 8k rows."""
    return (
        spark.range(0, N_DRIVERS)
        .select(
            F.col("id").cast("int").alias("driver_id"),
            F.concat(F.lit("driver-"), F.col("id")).alias("driver_name"),
            F.element_at(
                F.array(F.lit("hack"), F.lit("livery"), F.lit("black-car")),
                (F.pmod(F.col("id"), F.lit(3)) + 1).cast("int"),
            ).alias("licence_class"),
        )
    )
