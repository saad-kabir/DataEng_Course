# Day 3 · expected numbers and worked answers

Measured on an Apple Silicon laptop against the stack as shipped. Shapes to expect,
not targets.

---

## Lab 3.1 — an hourly pipeline you can re-run

Interval 2024-01-15 08:00 → 09:00 holds **2,342** trips.

| Step | appended | merged | distinct trip_ids |
|---|---|---|---|
| after first run | 2,342 | 2,342 | 2,342 |
| after re-run | **4,684** | **2,342** | 2,342 |
| after 3-day backfill | grows | grows | equals merged |

The one number to put on the projector: 4,684 rows for 2,342 distinct trips, and
Airflow green throughout.

Backfill of 72 intervals at `max_active_runs=2` takes roughly 6-10 minutes. Peak
connections to `rides` land around 8-12; the point is that it is bounded and
observable, not the exact figure.

**Discuss answers**

1. *Where a duplicate-row bug gets caught.* Nowhere, until a metric looks wrong —
   which is usually a downstream consumer noticing revenue doubled, days later. That
   is the argument for a uniqueness test on the key running as part of the pipeline,
   not for more careful operators.
2. *What else a 90-day backfill exhausts.* Source database connections and its WAL,
   the warehouse's concurrency slots or credits, object-store request quotas, and the
   scheduler's own worker slots — which starves every other DAG on the platform.
3. *`@hourly` on Airflow 3.* The UI would show green runs with a data interval whose
   start equals its end. The data would show an empty table and no error anywhere.
   This is the most dangerous kind of bug: a pipeline that reports success and does
   nothing.

---

## Lab 3.2 — CDC into bronze, tested

| Check | Value |
|---|---|
| Rows in initial snapshot | **200** |
| Topic offset after update + delete | **202** |
| Last two ops | `u` then `d` |
| Retained WAL after 200k-row update on an **uncaptured** table | **~102 MB** |

Change event shapes:

```
op=u  before={trip_id:812, fare:22.0}  after={trip_id:812, fare:44.1}
op=d  before={trip_id:813, fare:23.0}  after=null
```

**The 102 MB is the lesson.** The `trips` table is not in `table.include.list`, and
its WAL is still retained behind the slot, because Postgres cannot know what the
consumer will need until it reads it. One paused connector on one 200-row table can
fill a disk.

Do not promise the retained figure collapses the instant you resume. It falls once
Debezium consumes the backlog *and flushes offsets*, which is periodic. On an idle
database it can tick up first.

**Discuss answers**

1. *Query-based ingestion and row 813.* A high-water mark on `updated_at` never sees
   a hard delete. Row 813 stays in the warehouse forever, and no amount of re-running
   fixes it — the evidence is gone from the source.
2. *The setting that made `fare` readable.* `decimal.handling.mode: double`. Without
   it, Postgres `numeric` arrives as base64 `VariableScaleDecimal` bytes.
3. *What "connector up" misses.* Everything that matters: slot lag, snapshot stalls,
   a connector consuming but failing to commit, and schema changes it cannot decode.

---

## Lab 3.3 — a type 2 dimension that holds up

| Driver | Versions | Why |
|---|---|---|
| 1 | **2** | city changed — a tracked column |
| 2 | **1** | only `phone` changed, which is not hashed |
| 3 | 1 | no change |

| Method | Manhattan | Brooklyn | Queens |
|---|---|---|---|
| historical | **100.0** | 290.0 | 60.0 |
| naive current-row | **absent** | **390.0** | 60.0 |

Manhattan does not appear at all in the naive result. Both columns total 450. Nothing
errors, nothing is null. Say that out loud — the room tends to expect a null or a
crash.

**Discuss answers**

1. *What catches it.* A reconciliation against a known-good prior period, or a test
   asserting that a closed accounting period's totals never change. No schema test
   catches it, because the schema is fine.
2. *Merge and insert not atomic.* A reader between the two statements sees driver 1
   with **zero** current rows, so every fact for that driver drops out of an inner
   join. Iceberg's atomic commit is what makes the pair safe.
3. *Which attributes are type 1.* Corrections and cosmetics — spelling fixes, phone,
   email. The test is slide 26's: when this changes, should last quarter's numbers
   change too? Yes means type 1.

---

## Lab 3.4 — small files, compaction, rollback

| Stage | Data files | Avg file size | Planning | Execution |
|---|---|---|---|---|
| 500 micro-batches | **500** | 1,271 B | **80.46 ms** | 662.17 ms |
| after compaction | **1** | 137,521 B | **7.67 ms** | 157.31 ms |

Planning **10.5×** faster, execution **4.2×**. Zero rows changed.

| Stage | Rows | Snapshots |
|---|---|---|
| before damage | 50,000 (sum 1,962,500.0) | 502 |
| after DELETE of one day | 48,200 | 503 |
| after rollback | 50,000 (sum 1,962,500.0) | 504 |
| after expiry | 50,000 | **1** |

The rollback that worked a minute earlier then fails with
`Cannot roll back to unknown snapshot id`.

Generating the 500 micro-batches takes about **80 seconds**. Warn the room before they
start typing so nobody assumes it has hung.

Use `EXPLAIN ANALYZE`, not wall time. At 50,000 rows the wall-clock difference is
mostly CLI startup; the planning figure is the honest one and it is the number the
deck's claim is actually about.

**Discuss answers**

1. *Which grows at 1,000× scale.* Planning grows with **file count**, execution with
   **data volume**. A table 1,000× larger with the same file sizes has 1,000× the
   manifests, so planning is the one that becomes pathological — it is why the symptom
   is "a trivial query takes seconds to plan".
2. *What expiry deletes.* The snapshot metadata, and then any data file no surviving
   snapshot references. Rollback only ever moved a pointer, so once the pointer's
   target is gone there is nothing to point at. Irreversible by construction.
3. *Who owns maintenance.* The honest answer on most platforms is nobody, and the
   symptom arrives months later as unexplained slowness. The dashboard slide 36 asks
   for — file count and average file size per table — is the earliest warning.
