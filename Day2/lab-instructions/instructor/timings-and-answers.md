# Day 2 · expected numbers and worked answers

All numbers below were measured on an Apple Silicon laptop, `local[*]` with 10 cores,
16 GB RAM, on the stack as shipped. Treat them as the shape to expect, not a target.

---

## Lab 2.1 — read the plan, then let AQE re-plan it

`trips` is 2,917,230 rows after the borough join; `drivers` is 8,000.

| Run | Setting | Exchanges | Post-shuffle partitions | Join | Elapsed |
|---|---|---|---|---|---|
| A | adaptive off | 5 | 64 (fixed) | SortMergeJoin ×2 | **2.5 s** |
| B | adaptive on | 10 | coalesced | SortMergeJoin ×4 | **1.3 s** |
| C | AQE + broadcast | 6 | coalesced | BroadcastHashJoin ×4 | **0.7 s** |

The Exchange counts go **up** with AQE on, which surprises people. AQE re-plans and
the executed plan carries the adaptive sub-plans; it is not doing more shuffling.
Point at the stage count in the UI, not the node count in the string.

**Discuss answers**

1. *Run C is fastest and can also fail outright.* A broadcast that does not fit
   OOMs the driver, and the whole job dies — where an unnecessary sort-merge join is
   merely slow. Before raising the threshold: check the real size of the small side
   after filtering and projection (not the table size), and confirm executor and
   driver memory can hold it once per executor.
2. *What AQE cannot fix.* Anything decided before the first shuffle boundary: the
   file layout it has to scan, whether a filter can be pushed down, the partitioning
   of the source data. AQE re-plans with measured statistics, but it cannot go back
   and re-write data that was laid out badly on Day 1 — which is the link back to
   lab 1.4.
3. *What grows and what shrinks at scale.* The A-to-B gap grows: 64 tasks of
   scheduling overhead is noise on a laptop and real money across a cluster with
   network shuffle. The B-to-C gap shrinks in relative terms once the broadcast has
   to cross a network to every executor.

**Stretch.** At `shuffle.partitions=2000`, run A degrades sharply (thousands of
near-empty tasks); run B is almost unchanged because AQE coalesces them away. The
sentence you are looking for: AQE removes the penalty for setting it *too high*, not
the penalty for setting it too low — it can merge partitions, never split them
before the fact.

---

## Lab 2.2 — fix a skewed join

Skewed `driver_id` distribution, seeded so everyone matches:

```
0     1,749,399     <- 60%
5650      2,375
6421      2,350
```

| Run | Fix | Elapsed | Plan evidence |
|---|---|---|---|
| 1 | none, adaptive off | **3.4 s** | `AQEShuffleRead none` |
| 2 | AQE skew join, threshold 4m | **1.4 s** | `['coalesced', 'coalesced and skewed']` |
| 3 | explicit salt ×16 | **1.6 s** | dimension 8,000 → 128,000 |

**The thing to labour.** `skewJoin.enabled` is already `true` by default and does
nothing until the threshold is lowered, because the default
`skewedPartitionThresholdInBytes` is 256 MB and the hot partition here is tens of
megabytes. Someone will conclude the feature is broken. It is doing exactly what it
was configured to do.

Expect max/median task duration around **20-40×** in run 1 and **3-6×** in run 2.
These vary a lot by machine; the ratio dropping is what matters, not the values.

**Discuss answers**

1. *Where AQE is not enough.* When the skew is in a plain aggregation rather than a
   join (no build side to replicate), when the hot key needs a bigger split factor
   than AQE picks, or when the skew is in a window function's partition key.
2. *Who the 256 MB default is for.* Clusters with multi-gigabyte shuffles. The
   lesson is that tuning advice carries an implicit scale, and applying cluster
   defaults to small data silently disables features.
3. *What isolating hot keys buys.* Total control of the plan for the hot key
   (broadcast it) while the long tail takes the normal path — and it works when you
   know the hot keys in advance. It costs a union, two code paths, and a list of hot
   keys that goes stale.

---

## Lab 2.3 — partitions, groups, offsets

`day2_make_events.py` writes **2,405 events**, event time 08:00:01 to 08:59:58.
Partition offsets after producing, roughly: 605 / 635 / 557 / 608.

| Members | Partitions each | Idle |
|---|---|---|
| 2 | 2, 2 | 0 |
| 3 | 2, 1, 1 | 0 |
| 5 | 1, 1, 1, 1, 0 | **1** |

Rebalance pause in step 4 is typically **1-3 seconds** with
`GROUP_INITIAL_REBALANCE_DELAY_MS=0` (set in compose). The default 3 s delay makes it
feel like a hang, which is why it is overridden.

**Discuss answers**

1. *What rising lag does not tell you.* Whether the consumer is slow, the producer
   sped up, a rebalance is in progress, or one partition is hot. Next: lag **per
   partition** (is it one or all?), then consumer processing time versus poll interval.
2. *Cost of over-provisioning partitions.* More files and open handles per broker,
   more memory in producers' buffers, longer rebalances, and more consumer
   connections — plus every partition is a unit of ordering you can never merge back.
3. *What keying by `trip_id` gives up.* Per-city ordering. Two events for the same
   city land on different partitions and have no relative order, so you cannot
   reconstruct "what happened in Manhattan, in sequence" from the log alone.

---

## Lab 2.4 — windows, lateness, recovery

| Checkpoint | Value |
|---|---|
| Windows emitted before late event | **148** |
| Windows emitted after late event | **148** (unchanged) |
| Job state 10 s after `kill` | often **RUNNING** — see below |
| Job state 60 s after `kill` | **RESTARTING**, and it stays there |
| TaskManagers / slots once settled | **1 TM, 2 slots**, job needs 4 |
| Windows after recovery | **148** |
| Duplicate window keys | **0** |

**The flapping is the part to prepare for.** For roughly the first 40 seconds after
the kill, the job alternates between `RESTARTING` and `RUNNING` — the JobManager has
not yet had a heartbeat timeout, still counts the dead TaskManager's two slots,
schedules onto them, and fails. Only after ~50 s does `/overview` drop to
`taskmanagers: 1, slots-total: 2` and the job settle into `RESTARTING`.

Someone will check at ten seconds, see `RUNNING`, and say the lab does not work. Say
up front: watch it for a full minute. It is also the best argument you will get all
week for why an alert on job state alone is not enough.

The 08:59 window never fires — no event arrives after it, so the watermark never
passes its end. Someone always asks. It is the correct behaviour and the best
illustration in the lab of what "unbounded" means.

**Why there are no duplicates**, despite the sink being at-least-once by default: the
job restarts from a checkpoint that already recorded both the source offsets and the
fact that those windows had fired, so nothing is re-processed. Do not let the room
conclude that a plain Kafka sink is exactly-once — it is not, and the stretch makes
that concrete. The honest statement is: *replay did not happen here, so the sink's
weakness was not exercised.*

**Discuss answers**

1. *Who pays for out-of-orderness.* Every downstream consumer, in latency. Five
   seconds of allowed lateness is five seconds every window is held before anyone
   sees it — paid always, to handle events that arrive rarely.
2. *A correct "last minute" dashboard.* You cannot have one from event-time windows
   alone. Either emit early with a processing-time trigger and correct later, or
   publish the watermark alongside the data so the dashboard can say "complete up to
   08:58" rather than implying completeness it does not have.
3. *What would break recovery.* A non-replayable source, or a sink with side effects
   that are not idempotent — sending an email per window, incrementing an external
   counter, calling a payment API.

---

## Lab 2.5 — point in polygon, three ways

263 zones, **99,498** generated points across 253 zones — with the standard
2,000,000 rows in `trips`. The point count is `trips` row count / 20 per zone, so if
someone ran `load_postgres.py` twice they will have ~110,000 points and proportionally
slower phase 1. Worth asking if their numbers look 10% off.

| Phase | Plan | Execution time | Notes |
|---|---|---|---|
| 1 no index | Nested Loop + Seq Scan on zones | **10-20 s** | est. 54,076 rows vs 99,498 actual |
| 2 GiST index | Index Scan, `Rows Removed by Filter: 1` | **~1.5 s** | **10-12×** |
| 3 subdivided | Index Scan on `zones_sub` | **~1.2 s** | **1.25×** more, **~14×** total |

**Phase 1 varies widely** — 9 s to 18 s on the same machine — depending on whether
Postgres picks a parallel plan, which depends on what else is running. If marimo and
Flink are still up, expect the slow end. Do not let anyone treat their phase-1 number
as comparable with their neighbour's; the *ratios* are what compare.

Subdivision: 263 zones → **616 pieces**, average vertices **373 → 161**.

Ground truth check: 99,498 matched, 99,498 agree, **0 disagree**.

**Discuss answers**

1. *Index or subdivide, if only one.* The index, every time — 10× against 1.4×.
   Subdividing wins bigger when polygons are large and vertex-heavy (census tracts,
   coastlines, river catchments); with 263 compact urban zones there is less to gain.
2. *The shape that explodes the fan-out.* Anything long and diagonal: a road, a
   river, a flight path. Its bounding box covers an enormous area it does not occupy,
   so phase 1 returns thousands of candidates phase 2 must reject one at a time.
   Fix that one polygon by subdividing it, not the whole table.
3. *Before reaching for a grid.* Whether the answer tolerates boundary error, and how
   much. The error scales with perimeter, not area, so small zones with long
   perimeters — exactly the ones people bill per zone — are where it is worst.

**Stretch.** `ST_Simplify(geom, 0.0001)` is roughly 11 m of tolerance; expect a small
further speedup and a few hundred points changing zone. The point of the exercise is
that they quantify it before deciding, not that the number is interesting.
