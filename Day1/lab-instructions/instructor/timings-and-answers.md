# Expected numbers and worked answers

Measured on an M2 MacBook Pro, 16 GB, Docker Desktop with 8 GB allocated. Treat these
as the right order of magnitude, not a target.

---

## Lab 1.1 · Replication and failover

| Measurement | Expected |
|---|---|
| Peak rows behind | 200,000 to 900,000 |
| Peak `replay_lag_bytes` | 40 MB to 250 MB |
| `replay_lag` at peak | 1 to 6 seconds |
| WAL retained after 60 s paused | 300 MB to 900 MB |
| `pg_wal` size, paused | 1 GB or more |
| Catch-up after unpause | 10 to 40 seconds |

**Discuss 1.** Nothing in the connection layer distinguishes a fresh replica from one
six hours behind — both answer queries successfully. The catch has to be an explicit
freshness check: either the application reads `pg_last_xact_replay_timestamp()` and
compares it to now, or a monitor alerts on `replay_lag` and the read pool is removed
from rotation. This is the same argument as publishing freshness next to every cached
number, which returns on Day 5.

**Discuss 2.** The slot pins WAL on the primary indefinitely. `pg_wal` grows until the
disk fills, at which point the *primary* stops accepting writes. A replica outage
becomes a primary outage. Fix: `max_slot_wal_keep_size`, which lets the primary drop
the slot rather than run out of disk, and an alert on retained WAL.

**Discuss 3.** Bytes convert to time only with a write-rate estimate, and write rate is
not constant. The useful metric is `pg_last_xact_replay_timestamp()` on the replica,
which answers the staleness question directly.

---

## Lab 1.2 · Indexes and plans

| Step | Plan node | Buffers | Time |
|---|---|---|---|
| 1 no index | Parallel Seq Scan | ~28,000 shared read | 180-400 ms |
| 2 wrong index | Parallel Seq Scan (unchanged) | same | same |
| 3 matching index | Bitmap Heap Scan + Bitmap Index Scan | ~900 | 8-25 ms |
| 4 covering index | Index Only Scan, Heap Fetches: 0 | ~120 | 2-8 ms |
| 5 function on column | Parallel Seq Scan | ~28,000 | 180-400 ms |

`idx_trips_payment` is about 13 MB and never used.
INSERT of 100k rows: roughly 1.6 s with three indexes, 0.6 s with none.

**Discuss 1.** `pg_stat_user_indexes` with `idx_scan = 0`, checked after a full
business cycle including month-end. Caveat worth raising: an index backing a unique
constraint shows zero scans and must not be dropped.

**Discuss 2.** The covering index is larger than the plain one — roughly 60 MB against
45 MB here — because `total_amount` is stored in the leaf pages. It also has to be
rewritten whenever `total_amount` changes, so an INCLUDE column that updates
frequently is a bad trade.

**Discuss 3.** All three, in that order of cost. Cheapest is a lint rule for functions
applied to indexed columns in WHERE clauses. Most reliable is asserting the plan node
in CI for the queries that matter, which is exactly the benchmark gate built on Day 5.

---

## Lab 2.1 · Object store and planning

| Measurement | Expected |
|---|---|
| Upload 2,000 small objects | 25-70 s |
| List 2,000 objects | 0.4-1.2 s |
| List 1 object | 20-60 ms |
| Bytes, 3-column GROUP BY | ~95 MB |
| Bytes, `count(*)` with filter | ~24 MB |
| Ratio | 3-4× |

Row count: **2,964,624** for yellow taxi January 2024.

**Discuss 1.** There is no rename. "Renaming a folder" is a copy of every object to a
new key followed by a delete of every old one — two operations per object, charged,
non-atomic, and interruptible halfway. This is why table formats keep a manifest and
never rely on paths meaning anything.

**Discuss 2.** The filter has to be expressible in metadata the planner reads before
opening files: a partition value in the path, or min-max statistics in the footer that
exclude the file. Lab 2.2 builds both.

**Discuss 3.** A year of hourly partitions is 8,760 prefixes. At the listing cost
measured here, planning alone runs into seconds before a byte of data is read — and
that cost is paid on every query, not once.

---

## Lab 2.2 · Layout experiment

| Layout | Objects | Stored | Splits | Input rows | Physical bytes | Time |
|---|---|---|---|---|---|---|
| A flat | 4-8 | 52 MB | 8 | 2,964,624 | 52 MB | 2.4 s |
| B by dt | 31-62 | 54 MB | 14 | 668,000 | 12 MB | 0.7 s |
| C sorted | 31-62 | 51 MB | 14 | 668,000 | 3.1 MB | 0.4 s |
| D by zone | 265-800 | 71 MB | 265+ | 2,964,624 | 48 MB | 6.8 s |

Bytes read A vs C: **about 17× less**. Stored bytes B vs C: **within 6%**, and C is
usually *smaller* because sorting improves Parquet's dictionary and RLE encoding.

**Discuss 1.** The sort costs a shuffle at write time — on this dataset a few seconds,
on a daily job at production scale it can be a meaningful part of the run. It is paid
once by the pipeline and recovered on every read. That trade is only correct when
reads outnumber writes, which for a gold table they do by orders of magnitude.

**Discuss 2.** Partitioning by `pu_zone_id` is right when nearly every query filters
to a single zone and never scans a date range — a per-zone operational dashboard, for
instance. You would know from query logs: parse the WHERE clauses of the last month
of queries and count which columns appear.

**Discuss 3.** Partition on the column that appears in the most queries *and* has
moderate cardinality (usually date). Sort on the highest-cardinality column that
appears in the most remaining filters. The third column gets nothing, and that is the
honest answer to give the team.

---

## Lab 3.1 · Model and traverse

| Query | Rows | db hits | Time |
|---|---|---|---|
| Direct from LIS | 110-140 | ~300 | 15-40 ms |
| Two hops, excluding direct | 50 (limited) | 40,000-90,000 | 80-250 ms |
| shortestPath LIS to KTM | 1 | ~2,000 | 20-60 ms |

LIS to KTM is normally **3 legs**, commonly via a Gulf or European hub.

### The SQL equivalent of the two-hop query

```sql
SELECT DISTINCT d.iata, d.city, d.country
  FROM routes r1
  JOIN routes r2 ON r2.src_iata = r1.dst_iata
  JOIN airports d ON d.iata = r2.dst_iata
 WHERE r1.src_iata = 'LIS'
   AND r2.dst_iata <> 'LIS'
   AND NOT EXISTS (
         SELECT 1 FROM routes direct
          WHERE direct.src_iata = 'LIS'
            AND direct.dst_iata = r2.dst_iata)
 ORDER BY d.country, d.city;
```

Readable at two hops. The point to make: three hops adds another self-join and another
correlated NOT EXISTS, four hops adds another, and a variable-length path has no SQL
form at all without a recursive CTE that most analysts will not write correctly.

**Discuss 1.** The NOT EXISTS. The joins are mechanical; expressing "and not already
reachable directly" is what makes the SQL hard to read and hard to extend.

**Discuss 2.** Unbounded, the search explores the whole reachable component — on this
graph, most of the world within five hops. A timeout kills the query after the work is
done; a bound stops it being started. The bound also encodes a real business rule:
nobody books a six-leg itinerary.

**Discuss 3.** Anything aggregate. Trips per airport per month, revenue by carrier,
on-time rates — these are columnar scans, and Postgres or a warehouse beats a graph
database on every one of them. Use the graph for the questions about *paths*.

---

## Lab 3.2 · Bulk load and project

| Measurement | Expected |
|---|---|
| Routes load, no index | 3-8 minutes |
| Routes load, with constraint | 20-45 seconds |
| Speedup | **8× to 20×** |
| Projection memory | 3-8 MB |
| Projected nodes / rels | ~6,000 / ~37,000 |
| Cypher `shortestPath()` | 20-60 ms |
| GDS Dijkstra | 5-20 ms plus projection cost |

Top by PageRank: typically **FRA, CDG, AMS, IST, ATL, PEK, LHR** in some order.
Top by out-degree: **ATL, ORD, PEK, LHR, CDG, FRA** — heavy US and Chinese domestic
hubs rank higher on raw degree than on PageRank.

**Discuss 1.** It is the same mistake as lab 1.2 step 1: a predicate on an unindexed
column, executed once per row of input. Neo4j calls it `NodeByLabelScan`, Postgres
calls it `Seq Scan`, and in both cases the fix is an index on the lookup key. Worth
drawing that line explicitly — it lands better than either lab alone.

**Discuss 2.** Nothing breaks and no error is raised, which is the problem. The
algorithm returns a correct answer about a graph that no longer exists. Anything
written back with `.write` is therefore as of projection time, and that timestamp
belongs in the property name or in metadata. This is the same as-of reasoning as
point-in-time correctness on Day 5.

**Discuss 3.** PageRank, with the explanation that degree counts how many places you
can fly to, and PageRank counts how much of the network you can *reach through* the
places you fly to. A network planner cares about the second. Show one airport where
they disagree sharply — usually a well-connected mid-size European hub.

---

## Time checks

| Lab | Median finish | Slowest 20% |
|---|---|---|
| 1.1 | 35 min | 50 min |
| 1.2 | 40 min | 50 min |
| 2.1 | 40 min | 55 min |
| 2.2 | 45 min | 60 min+ |
| 3.1 | 35 min | 45 min |
| 3.2 | 45 min | 60 min+ |

Labs 2.2 and 3.2 are the two that overrun. Both have a slow cell that people watch
rather than work through — start them 5 minutes early if the schedule allows.
