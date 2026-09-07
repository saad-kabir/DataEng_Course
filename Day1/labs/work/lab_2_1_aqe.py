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
    # Lab 2.1 - Read the plan, then let AQE re-plan it

    Join January's taxi trips to the driver dimension and aggregate by city, three
    ways. The query never changes. Only the planner's freedom changes.

    | Run | Setting | What you are looking for |
    |---|---|---|
    | A | `adaptive.enabled=false` | exactly `shuffle.partitions` post-shuffle partitions |
    | B | `adaptive.enabled=true`, no broadcast | `AQEShuffleRead coalesced` in the plan |
    | C | AQE on, broadcast allowed | the join flips to `BroadcastHashJoin` |

    Run the cells in order. Leave the Spark UI open at
    **http://localhost:4040** - the SQL tab is where this lab actually happens.
    """)
    return


@app.cell
def _():
    import sys, time

    sys.path.insert(0, "/work")
    from day2_common import spark_session, trips, drivers
    from pyspark.sql import functions as F

    spark = spark_session("lab-2.1-aqe")
    # 64, not the 200 default, so run A's partition count is legible in the UI
    # without scrolling. The lesson is that it is a fixed number, not which number.
    spark.conf.set("spark.sql.shuffle.partitions", "64")

    t = trips(spark)
    d = drivers(spark)
    print(f"trips   {t.count():,} rows")
    print(f"drivers {d.count():,} rows")
    return F, d, drivers, spark, t, time, trips


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## The query

    One join, one aggregation. Written once, run three times.
    """)
    return


@app.cell
def _(F, d, t):
    def query(trips_df, drivers_df):
        return (
            trips_df.join(drivers_df, "driver_id")
            .groupBy("city")
            .agg(F.sum("fare").alias("total_fare"), F.count("*").alias("trips"))
        )

    def run(spark, label, adaptive, broadcast_bytes, trips_df=t, drivers_df=d):
        """Time the same query under one configuration and report the plan nodes."""
        import time

        spark.conf.set("spark.sql.adaptive.enabled", adaptive)
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", broadcast_bytes)
        q = query(trips_df, drivers_df)
        t0 = time.time()
        rows = q.collect()
        elapsed = time.time() - t0
        plan = q._jdf.queryExecution().executedPlan().toString()
        readers = sorted(
            {
                line.strip().split("AQEShuffleRead")[1].strip()[:30]
                for line in plan.split("\n")
                if "AQEShuffleRead" in line
            }
        )
        print(f"--- {label}")
        print(f"    elapsed              {elapsed:6.1f}s   ({len(rows)} cities)")
        print(f"    SortMergeJoin        {plan.count('SortMergeJoin')}")
        print(f"    BroadcastHashJoin    {plan.count('BroadcastHashJoin')}")
        print(f"    Exchange             {plan.count('Exchange')}")
        print(f"    AQEShuffleRead       {readers or 'none'}")
        return elapsed

    return query, run


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Run A - adaptive execution off

    The post-shuffle partition count is whatever `spark.sql.shuffle.partitions` says,
    regardless of how much data actually arrives. Six cities come out of a 64-partition
    shuffle. In the UI's SQL tab, the Exchange node reports 64.
    """)
    return


@app.cell
def _(run, spark):
    a = run(spark, "A  adaptive off", "false", "-1")
    return (a,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Run B - adaptive execution on

    Same query. Catalyst now re-plans at each shuffle boundary using the sizes it just
    measured, and collapses those mostly-empty partitions.

    The node is called **`AQEShuffleRead`**. Slide 12 calls it `CustomShuffleReader`,
    which was its name in Spark 3.0 and 3.1 - this stack runs Spark 4, where it was
    renamed. Same node, same job.
    """)
    return


@app.cell
def _(run, spark):
    b = run(spark, "B  adaptive on, broadcast disabled", "true", "-1")
    return (b,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Run C - let the planner broadcast

    Runs A and B forced a sort-merge join by setting the auto-broadcast threshold to
    `-1`. Put it back to 10 MB and AQE discovers, at runtime, that the driver side is
    small - and switches join strategy mid-query.
    """)
    return


@app.cell
def _(run, spark):
    c = run(spark, "C  adaptive on, broadcast allowed", "true", "10485760")
    return (c,)


@app.cell(hide_code=True)
def _(a, b, c, mo):
    mo.md(f"""
    ## Your three numbers

    | Run | Elapsed |
    |---|---|
    | A  adaptive off | **{a:.1f}s** |
    | B  adaptive on | **{b:.1f}s** |
    | C  broadcast allowed | **{c:.1f}s** |

    Copy these into the Record table in the lab sheet, then open
    **http://localhost:4040/SQL/** and compare the three plans visually.

    A caution before you generalise: this is one laptop running `local[*]` over 3
    million rows. The *plan* differences here are exactly what you would see on a
    cluster. The *wall-clock* differences are much smaller than they would be at
    scale, where the shuffle crosses a network instead of a memory bus.
    """)
    return


if __name__ == "__main__":
    app.run()
