# Lab 2.1 · Read the plan, then let AQE re-plan it

**Time** 50 minutes · **Slide 12** · **Systems** Spark (in marimo)
**Notebook** `Day1/labs/work/lab_2_1_aqe.py`

## Objective

Run one join-and-aggregate three times, changing nothing but what the planner is
allowed to do, and read the difference out of the physical plan rather than the
clock.

## Before you start

The stack is up and every service reads `healthy`:

```bash
cd DataEng_Course/Day1/labs
docker compose ps
```

Open marimo at **http://localhost:8888** and open `lab_2_1_aqe.py`.

Open a second browser tab at **http://localhost:4040**. That is the Spark driver UI,
served from inside the marimo container. It is empty until the notebook's first cell
creates a session, and it disappears when the session stops.

## The data

The notebook builds two frames from January's taxi extract:

| Frame | Rows | Notes |
|---|---|---|
| `trips` | ~2.9 M | `city` is the real borough of the pickup zone; `driver_id` is a deterministic hash |
| `drivers` | 8 000 | a small dimension: `driver_name`, `licence_class` |

The query is the same every time:

```python
trips.join(drivers, "driver_id").groupBy("city").agg(sum("fare"), count("*"))
```

## Steps

### 1. Run the notebook down to the end of run A (15 min)

Run A sets `spark.sql.adaptive.enabled=false` and
`spark.sql.autoBroadcastJoinThreshold=-1`, forcing a plain sort-merge join with no
adaptive help at all.

`spark.sql.shuffle.partitions` is set to **64** rather than the 200 default, only so
the number is easy to see in the UI. The lesson is that it is a *fixed* number, not
which number it is.

In the Spark UI, open **SQL / DataFrame → the completed query → the Exchange node**.
It reports 64 partitions. Six cities came out of it. Fifty-eight of those partitions
did nothing and still cost a task each.

Record the elapsed time the cell prints.

### 2. Run B - turn adaptive execution on (10 min)

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

Same query. Now look at the printed plan summary:

```
AQEShuffleRead   ['coalesced']
```

> **The deck says `CustomShuffleReader`.** That was the node's name in Spark 3.0 and
> 3.1. This stack runs Spark 4, where it is called **`AQEShuffleRead`**. Same node,
> same job: it reports how many post-shuffle partitions it merged.

In the UI, the same query now shows an `AQEShuffleRead` node between the Exchange and
the aggregate, and the task count for that stage has collapsed.

### 3. Run C - let the planner broadcast (10 min)

Runs A and B disabled broadcasting with `autoBroadcastJoinThreshold=-1`. Put it back
to 10 MB:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
```

The plan summary flips:

```
SortMergeJoin        0
BroadcastHashJoin    4
```

The driver side is 8 000 rows. AQE learns its *real* size after the first stage runs
and switches join strategy mid-query - the decision slide 15 describes, made from
measurement rather than from an estimate.

### 4. Read the three plans side by side (15 min)

In the Spark UI, all three runs are listed in the SQL tab. Open them in three tabs
and compare the same three things in each: the number of Exchange nodes, the
post-shuffle partition count, and the join operator.

## Verify

The notebook's last cell prints your three numbers. A correct run looks like this,
with the middle column varying by machine:

```
A  adaptive off               2.5s    SortMergeJoin, no AQEShuffleRead
B  adaptive on                1.3s    SortMergeJoin, AQEShuffleRead coalesced
C  broadcast allowed          0.7s    BroadcastHashJoin
```

What must be true regardless of your timings: **A has no `AQEShuffleRead` line, B
has one, and C has replaced every `SortMergeJoin` with `BroadcastHashJoin`.**

## Record

| Run | Setting | Exchanges | Post-shuffle partitions | Join operator | Elapsed |
|---|---|---|---|---|---|
| A | adaptive off | | 64 | | |
| B | adaptive on | | | | |
| C | AQE + broadcast | | | | |

Then answer in one line each:

- A to B, what did AQE actually change — the work done, or the way it was scheduled?
- Which of the three would you ship, and what would make run C dangerous?
- `shuffle.partitions` was 64 here. What would you set it to for a job whose output
  is 400 GB rather than six rows, and why is that question badly posed?

## Discuss

1. Run C is the fastest and also the one that can fail outright. What is the failure,
   and what would you check before raising `autoBroadcastJoinThreshold` in production?
2. AQE re-plans at shuffle boundaries using statistics it measures. Which decisions
   can it therefore *not* fix, no matter how good those statistics are?
3. This ran on one laptop over three million rows. Which of the three differences you
   measured would grow on a real cluster, and which would shrink?

## Stretch

Set `spark.sql.shuffle.partitions` to 2000 and re-run A and B. A gets dramatically
worse; B barely moves. Then explain, in one sentence, why AQE makes the setting
matter less but not stop mattering.

## On GCP

Dataproc Serverless keeps the Spark UI after the job ends through the persistent
history server - turn it on before you need it, because you cannot turn it on
afterwards. The AQE settings are identical; only where you read the plan changes.
