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
    # Lab 2.2 - Fix a skewed join

    One driver takes 60% of January's trips. Join that to the driver dimension and
    the stage waits on a single task.

    | Run | Fix | What changes |
    |---|---|---|
    | 1 | none, adaptive off | one task holds 60% of the rows |
    | 2 | `adaptive.skewJoin.enabled=true` | the plan gains a skew-split reader |
    | 3 | explicit salt x16 | you control the split factor, not AQE |

    **Read this before you start.** Runs 2 and 3 will finish faster than run 1, but
    be careful what you attribute that to: most of the gain is AQE coalescing 64
    mostly-empty partitions, which you already saw in lab 2.1, not the skew split
    itself. The evidence that the *skew* fix worked is in the **plan** - the reader
    changes to `coalesced and skewed` - and in the **task duration spread** in the
    Spark UI. On ten local cores a single hot task is largely absorbed; on a cluster
    it is the whole job. Keep **http://localhost:4040** open.
    """)
    return


@app.cell
def _():
    import sys, time

    sys.path.insert(0, "/work/day2")
    from day2_common import spark_session, trips, drivers
    from pyspark.sql import functions as F

    spark = spark_session("lab-2.2-skew")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    # Force a sort-merge join for every run. An 8,000-row dimension would otherwise
    # broadcast, and a broadcast join has no shuffle on the big side - so no skew to
    # study. Skew is a property of the shuffle, not of the data on its own.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    d = drivers(spark)
    base_trips = trips(spark)

    # Deck slide 20: make one key hold 60% of rows. Seeded, so every learner in the
    # room gets the same distribution and the same numbers.
    skewed = base_trips.withColumn(
        "driver_id", F.when(F.rand(7) < 0.6, F.lit(0)).otherwise(F.col("driver_id"))
    )
    return F, base_trips, d, drivers, skewed, spark, sys, time, trips


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Confirm the skew before fixing it

    Diagnose first (slide 17). If the distribution is even, the problem is elsewhere
    and every fix below is wasted work.
    """)
    return


@app.cell
def _(F, skewed):
    (
        skewed.groupBy("driver_id")
        .count()
        .orderBy(F.desc("count"))
        .limit(5)
        .show(truncate=False)
    )
    return


@app.cell
def _(F, d, skewed):
    def run(spark, label, conf):
        import time

        for k, v in conf.items():
            spark.conf.set(k, v)
        q = (
            skewed.join(d, "driver_id")
            .groupBy("licence_class")
            .agg(F.sum("fare").alias("total_fare"), F.count("*").alias("trips"))
        )
        t0 = time.time()
        q.collect()
        elapsed = time.time() - t0
        plan = q._jdf.queryExecution().executedPlan().toString()
        readers = sorted(
            {
                line.strip().split("AQEShuffleRead")[1].strip()[:32]
                for line in plan.split("\n")
                if "AQEShuffleRead" in line
            }
        )
        print(f"--- {label}")
        print(f"    elapsed          {elapsed:6.1f}s")
        print(f"    AQEShuffleRead   {readers or 'none'}")
        return elapsed

    return (run,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Run 1 - baseline, adaptive off

    Open the SQL tab, click into the join stage, and sort tasks by duration. One bar
    far to the right, the rest bunched left. Shuffle read size per task follows the
    same ratio - that is what tells you it is skew and not a slow machine.
    """)
    return


@app.cell
def _(run, spark):
    r1 = run(spark, "1  baseline, adaptive off", {"spark.sql.adaptive.enabled": "false"})
    return (r1,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Run 2 - let AQE split it

    `skewJoin.enabled` is already `true` by default; on its own it does nothing here.
    AQE only splits a partition above
    `skewedPartitionThresholdInBytes`, which defaults to **256 MB** - and the hot
    partition in this dataset is a few tens of megabytes.

    So the threshold is lowered to match the data. That is the real lesson: the
    default is sized for cluster-scale partitions, and on smaller data the feature
    silently does nothing. Watch the reader change to `coalesced and skewed`.
    """)
    return


@app.cell
def _(run, spark):
    r2 = run(
        spark,
        "2  AQE skew join, threshold lowered to this data",
        {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "4m",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "4m",
        },
    )
    return (r2,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Run 3 - salt the key yourself

    Spread driver 0 across 16 partitions with a random suffix, and explode the
    dimension 16 ways so every salted key still finds its row. Adaptive is off, so
    nothing but the salt is doing the work.

    The cost is visible in the code: the dimension is now 16x larger, and the salt
    column has to be carried through the join. You would reach for this when the
    aggregation is not a join at all, or when you need a bigger factor than AQE chose.
    """)
    return


@app.cell
def _(F, d, skewed, spark):
    import time as _t

    SALT = 16
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    salted = skewed.withColumn("salt", F.floor(F.rand(42) * SALT).cast("int"))
    dim_exploded = d.withColumn(
        "salt", F.explode(F.array(*[F.lit(i) for i in range(SALT)]))
    )

    _t0 = _t.time()
    (
        salted.join(dim_exploded, ["driver_id", "salt"])
        .groupBy("licence_class")
        .agg(F.sum("fare").alias("total_fare"), F.count("*").alias("trips"))
        .collect()
    )
    r3 = _t.time() - _t0
    print(f"--- 3  explicit salt x{SALT}, adaptive off")
    print(f"    elapsed          {r3:6.1f}s")
    print(f"    dimension rows   {d.count():,} -> {dim_exploded.count():,}")
    return SALT, dim_exploded, r3, salted


@app.cell(hide_code=True)
def _(mo, r1, r2, r3):
    mo.md(f"""
    ## Your three numbers

    | Run | Elapsed |
    |---|---|
    | 1  baseline | **{r1:.1f}s** |
    | 2  AQE skew join | **{r2:.1f}s** |
    | 3  explicit salt | **{r3:.1f}s** |

    If those three numbers are close together, you have reproduced the lab correctly.
    Put them in the Record table, then answer the question that matters: **which of
    the three would you ship, and what would you need to measure on a real cluster
    to be sure?**
    """)
    return


if __name__ == "__main__":
    app.run()
