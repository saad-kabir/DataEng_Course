# Running Day 2

## Schedule

| Time | Block |
|---|---|
| 09:00 | Slides 1-11, chapter 1 lecture |
| 09:45 | **Lab 2.1** read the plan, then let AQE re-plan it |
| 10:35 | Break |
| 10:50 | Slides 13-19, tuning |
| 11:30 | **Lab 2.2** fix a skewed join |
| 12:20 | Lunch |
| 13:20 | Slides 22-30, chapter 2 |
| 14:05 | **Lab 2.3** partitions, groups, offsets |
| 14:55 | Break |
| 15:10 | Slides 32-39, Flink |
| 15:55 | **Lab 2.4** windows, lateness, recovery |
| 16:45 | Break |
| 16:55 | Slides 42-47, chapter 3 |
| 17:25 | **Lab 2.5** point in polygon, three ways |

There is **no Day 2 setup lab**. Yesterday's `docker compose up -d` brings up the
four new services. Say this in the first five minutes, because people will expect a
lab 0 and go looking for one.

## Say this before lab 2.1

Three things, or you will answer them twenty times:

1. **The Day 2 stack needs ~12 GB free, not 8.** Kafka plus three Flink containers
   add roughly 3 GB at idle. Tell anyone tight on memory to
   `docker compose stop neo4j trino` — nothing today touches either.
2. **The Spark UI is at http://localhost:4040** and only exists while a notebook
   holds a session. Labs 2.1 and 2.2 are half unusable without it.
3. **Stop each notebook before starting the next.** Two live Spark sessions in one
   marimo container fight over port 4040 and over memory, and the second one's UI
   silently lands on 4041 where nobody is looking.

## Where the deck and reality differ

Both are deliberate teaching moments in the sheets, but you should know them before
someone raises a hand.

- **Slide 12 says `CustomShuffleReader`.** That was the Spark 3.0/3.1 name. This
  stack is Spark 4 and the node is `AQEShuffleRead`. Lab 2.1 step 2 flags it.
- **Slide 40's SQL does not run.** `GROUP BY window_start, city` makes Flink plan a
  regular grouped aggregate, not a window aggregate, and the job fails to submit
  with `Table sink doesn't support consuming update changes`. It needs
  `GROUP BY window_start, window_end, city`. Lab 2.4 step 1 calls this out and
  suggests trying the broken form once on purpose — it is a good sixty seconds.
- **Slide 3's service list** includes Kafka Connect, Schema Registry and Sedona.
  None are in the stack: no Day 2 lab uses Connect or the registry (Connect arrives
  on Day 3 for CDC), and the Sedona lab was cut. Do not promise them.

## Demo live rather than making them run it

- **Lab 2.2, the Spark UI task histogram.** Put the stage page on the projector and
  sort by duration once. Everyone can then find it in their own UI; without the demo
  about a third will not.
- **Lab 2.4 step 4, the TaskManager kill.** Worth doing on the projector first so the
  room sees `RESTARTING` and the slot count together. Then let them do it themselves.
- **Lab 2.5 step 1, the CRS trap.** Run the load once with `-s 4326:4326` instead of
  `-s 2263:4326` and show the extent landing at longitude 900,000-ish. It takes
  ninety seconds and it is the most memorable thing in chapter 3.

## Failure modes that eat time

| Symptom | Cause | Fix |
|---|---|---|
| Spark UI is blank at :4040 | no live session, or a second notebook took 4041 | run the notebook's first cell; stop the other notebook |
| `Table sink doesn't support consuming update changes` | `window_end` missing from GROUP BY | add it |
| Flink job stuck `RESTARTING` forever | TaskManager not brought back after `kill` | `docker compose up -d flink-taskmanager-1` |
| "Lab 2.4 step 4 doesn't work, it says RUNNING" | checked before the heartbeat timed out | wait a full minute; it flaps for ~40 s first |
| `kafka-consumer-groups --reset-offsets` refuses | group still has live members | stop every consumer first |
| Consumers show no messages | consumed once already; no `--from-beginning` | add the flag, or reset offsets |
| Lab 2.5 step 3 takes 40 s not 10 s | `ANALYZE` not run, or machine under memory pressure | run `ANALYZE`; stop marimo and Flink |
| `shp2pgsql: command not found` | ran it on the host, not in the container | it is inside the postgres container |
| Points land in the ocean | `-s` flag wrong or omitted | reload with `-s 2263:4326` |

## Cut order when behind

1. **Lab 2.5 step 5** (subdivide). Phases 1 and 2 carry the lesson; phase 3 is the
   refinement. Demo the numbers instead.
2. **Lab 2.3 step 3** (the fifth idle consumer). State it and show the describe
   output from your own run.
3. **Lab 2.1 run C** (broadcast). Runs A and B are the AQE lesson; C is the bonus.
4. **Lab 2.2 run 3** (explicit salt). AQE's fix is the one they will actually use.

**Never cut lab 2.4.** It is the only place all week where event time, watermarks,
checkpointing and recovery are visible at once, and Day 3's CDC lab assumes they have
seen a job recover.

## What Day 2 leaves for Day 3

Kafka stays up with `trip_events` populated. Day 3's CDC lab adds Kafka Connect and
Debezium against the same broker, so do not tear the stack down at the end of the day.
