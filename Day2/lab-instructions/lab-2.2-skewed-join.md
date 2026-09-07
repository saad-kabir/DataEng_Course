# Lab 2.2 · Fix a skewed join

**Time** 50 minutes · **Slide 20** · **Systems** Spark (in marimo)
**Notebook** `Day2/labs/work/day2/lab_2_2_skew.py`

## Objective

Make one join key hold 60% of the rows, watch a single task hold the whole stage
open, then fix it twice - once by letting AQE split the partition, once by salting
the key yourself.

## Before you start

Lab 2.1 is complete, so you already know how to find a query in the Spark UI. Stop
the lab 2.1 notebook first (marimo's **Stop** button, or close its tab) - two live
Spark sessions in one container will fight over port 4040 and over memory.

Open `lab_2_2_skew.py` in marimo, and **http://localhost:4040** in a second tab.

## Read this before you run anything

Runs 2 and 3 will be faster than run 1. Be careful what you credit for that: most of
the gain is AQE coalescing 64 mostly-empty partitions, exactly as in lab 2.1 — not
the skew split.

The evidence that the *skew* fix worked is in two places, and neither is the clock:

- the plan, where the reader changes to `coalesced and skewed`
- the **task duration spread** for the join stage in the Spark UI

On ten local cores, one hot task is largely absorbed. On a cluster, one hot task
*is* the job. That difference is the lab.

## Steps

### 1. Confirm the skew before you fix it (10 min)

Slide 17 is emphatic about this: diagnose first, because if the distribution is even
then every fix below is wasted work.

The notebook skews `driver_id` so that driver 0 takes ~60% of trips, then counts:

```
+---------+-------+
|driver_id|count  |
+---------+-------+
|0        |1749399|
|5650     |2375   |
|6421     |2350   |
```

One key with 1.75 million rows; the runner-up has 2 375. That ratio, roughly 700:1,
is what you are about to make Spark deal with.

### 2. Run 1 - the baseline (10 min)

Adaptive off. The join is forced to a sort-merge join by
`autoBroadcastJoinThreshold=-1`: an 8 000-row dimension would otherwise broadcast,
and a broadcast join does not shuffle the big side at all — so there would be no
skew to study. **Skew is a property of the shuffle, not of the data on its own.**

While it runs, open the Spark UI → **Stages** → the join stage → **sort the task
table by Duration, descending**. You are looking for the shape slide 17 describes:
one bar far to the right, the rest bunched at the left, and *shuffle read size per
task following the same ratio*. That second part is what distinguishes skew from a
slow machine.

### 3. Run 2 - let AQE split it (10 min)

`spark.sql.adaptive.skewJoin.enabled` is **already `true` by default**, and on its
own it does nothing here. That is the most useful thing in this lab.

AQE only splits a partition larger than
`spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`, which defaults to
**256 MB**. The hot partition in this dataset is a few tens of megabytes. The
feature is on, the skew is real, and nothing happens.

The notebook lowers the threshold to match the data:

```python
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "4m")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "4m")
```

Now the printed plan summary changes:

```
AQEShuffleRead   ['coalesced', 'coalesced and skewed']
```

`coalesced and skewed` is the skew-split reader. Go back to the stage's task table:
the single long bar has become several medium ones.

### 4. Run 3 - salt the key yourself (10 min)

Adaptive off again, so nothing but the salt is doing the work. Driver 0 is spread
across 16 partitions with a random suffix, and the dimension is exploded 16 ways so
every salted key still finds its row:

```python
salted = skewed.withColumn("salt", floor(rand(42) * 16).cast("int"))
dim_exploded = drivers.withColumn("salt", explode(array(*[lit(i) for i in range(16)])))
salted.join(dim_exploded, ["driver_id", "salt"])
```

The cost is printed and is the point: the dimension goes from 8 000 rows to 128 000.
You pay that on every run, forever, whether or not the data is still skewed.

### 5. Decide (10 min)

Fill in the Record table and commit to an answer on which one you would ship.

## Verify

The notebook's last cell prints three numbers. Typical, on a laptop:

```
1  baseline            3.4s
2  AQE skew join       1.4s
3  explicit salt       1.6s
```

Your timings will differ. What must be true: **run 1 shows `AQEShuffleRead none`,
run 2 shows `coalesced and skewed`, and run 3's dimension row count is
8,000 -> 128,000.**

## Record

| Run | Fix | Max task duration | Median task duration | Ratio | Elapsed |
|---|---|---|---|---|---|
| 1 | none, adaptive off | | | | |
| 2 | AQE skew join | | | | |
| 3 | explicit salt x16 | | | | |

Max and median task duration come from the Spark UI stage page, not the notebook.

Then answer in one line each:

- The max-to-median ratio in run 1 was **____ ×**; in run 2 it was **____ ×**
- `skewJoin.enabled` was already true and did nothing. What would you have concluded
  if you had not checked the plan?
- Salting cost 16× the dimension. What has to be true about your data for that to be
  the right trade?

## Discuss

1. AQE fixed this without you knowing which key was hot. Name a case where that is
   not enough and you would have to salt.
2. The threshold default is 256 MB. Who is that default written for, and what does
   that tell you about applying cluster tuning advice to a laptop?
3. Slide 17 offers a third fix — isolate the hot keys and union the results. What
   does it buy that neither of the other two do, and what does it cost?

## Stretch

Implement the third fix: broadcast-join driver 0 on its own, sort-merge the other
7 999, and union the results. Compare against runs 2 and 3, and count the lines of
code you had to write and would now have to maintain.

## What this leaves for later

Nothing on disk. Stop the notebook before lab 2.3 so the Spark session releases its
memory - Kafka and Flink will want it.
