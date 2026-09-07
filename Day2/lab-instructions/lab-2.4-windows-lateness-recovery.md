# Lab 2.4 · Windows, lateness, recovery

**Time** 50 minutes · **Slide 40** · **Systems** Flink SQL, Kafka
**SQL** `Day1/labs/work/lab_2_4_windows.sql`

## Objective

Sum fares per city in one-minute event-time windows, prove that a late event changes
nothing and says nothing, then kill a TaskManager and show the totals come back
identical rather than doubled.

## Before you start

Lab 2.3 is complete and `trip_events` holds 2,405 events. Check:

```bash
cd DataEng_Course/Day1/labs
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic trip_events
```

The Flink cluster is up with all four slots free:

```bash
curl -s http://localhost:8081/overview
```

```json
{"taskmanagers":2,"slots-total":4,"slots-available":4,...}
```

Open the Flink dashboard at **http://localhost:8081** and leave it open.

## Steps

### 1. Create the sink topic and submit the job (10 min)

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic city_fares_1m --partitions 1 --replication-factor 1
```

Read `Day1/labs/work/lab_2_4_windows.sql` before you run it. Three things in it
matter:

```sql
WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
```

The whole lab follows from that line. It is a promise: *no event more than five
seconds older than the newest one I have seen will arrive.*

```sql
SET 'parallelism.default' = '4';
```

Four, to use all four slots — which is what makes step 3 have a consequence.

```sql
GROUP BY window_start, window_end, city
```

**Both** `window_start` and `window_end`. Slide 40 abbreviates this to `window_start`
alone; written that way Flink does not recognise a window aggregate at all, plans a
regular grouped aggregate that emits updates, and the job fails to submit with
`Table sink doesn't support consuming update changes`. Worth doing once on purpose.

Submit it:

```bash
docker compose exec flink-jobmanager ./bin/sql-client.sh -f /work/lab_2_4_windows.sql
```

```
Job ID: 47cedc00fa283f45ff839ad37bfddd25
```

Keep that job ID. In the dashboard the job is **RUNNING** and all four slots are in use.

### 2. Watch the windows close (10 min)

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic city_fares_1m --from-beginning
```

```json
{"window_start":"2024-01-15 08:00:00","window_end":"2024-01-15 08:01:00","city":"Manhattan","total_fare":933.36,"trips":35}
{"window_start":"2024-01-15 08:00:00","window_end":"2024-01-15 08:01:00","city":"Queens","total_fare":309.87,"trips":4}
```

One row per window per city. The job runs at parallelism 4, so **the order you see
them in will not match the order above** — four subtasks emit independently. The
values will match; the sequence will not. That is worth noticing now rather than
being surprised by it in a test that asserts on ordering. Note what is **not** there: the final window,
`08:59:00`. Nothing arrives after it, so the watermark never advances past its end,
so it never fires. A stream has no last window until you tell it what "last" means.

Count them:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic city_fares_1m
```

```
city_fares_1m:0:148
```

Write 148 down — it is the number step 3 and step 4 are checked against.

### 3. Inject a late event (10 min)

The watermark is now just past 08:59. Send an event timestamped two minutes in the
past, into a window that has already fired:

```bash
cd DataEng_Course/Day1/labs
echo 'trip-999999:{"trip_id":"trip-999999","city":"Manhattan","fare":999.99,"event_ts":"2024-01-15 08:57:00"}' > data/late_event.jsonl

docker compose exec kafka bash -c \
  "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
   --topic trip_events --reader-property parse.key=true --reader-property key.separator=: \
   < /data/late_event.jsonl"
```

Wait fifteen seconds, then count the sink again. **It is still 148.**

A 999.99 fare vanished. The job is still `RUNNING`. There is no error, no warning, no
metric moved that you were watching. That silence is the lesson: **late data policy
is something you choose, and if you do not choose it you have chosen to drop.**

### 4. Kill a TaskManager (15 min)

First confirm at least one checkpoint has completed — recovery has nothing to recover
to otherwise:

```bash
curl -s http://localhost:8081/jobs/<your-job-id>/checkpoints
```

Look for `"completed"` of 2 or more. Then:

```bash
docker compose kill flink-taskmanager-1
```

Watch the dashboard, and **keep watching for a full minute** — what happens is more
interesting than "it breaks".

For the first 40 seconds or so the job **flaps**, alternating between `RESTARTING`
and `RUNNING`:

```
t+5s    RESTARTING   t+30s   RUNNING
t+10s   RUNNING      t+40s   RESTARTING
t+20s   RESTARTING   t+50s   RESTARTING  <- and it stays there
```

Nothing is wrong with your cluster. The JobManager does not yet know the TaskManager
is dead: the container is gone, but the **heartbeat has not timed out**, so the
JobManager still counts its two slots as available, schedules onto them, fails, and
retries. It is repeatedly trying to place work on a corpse.

Once the heartbeat times out (~50 s), the TaskManager is deregistered and the truth
appears:

```bash
curl -s http://localhost:8081/overview
```

```json
{"taskmanagers":1,"slots-total":2,"slots-available":0,...}
```

Two slots for a job that needs four. **Now** it stays `RESTARTING`. This is the slot
starvation the cluster was deliberately sized to show you: recovery is not only a
matter of state, it is a matter of capacity, and a cluster with no spare capacity
cannot recover from losing a node.

If you check the state once, at ten seconds, you will see `RUNNING` and conclude
nothing happened. Failure detection is not instant, and monitoring that samples too
early lies to you.

Bring it back:

```bash
docker compose up -d flink-taskmanager-1
```

The job returns to **RUNNING**, restoring from the last checkpoint. Count the sink
one more time.

## Verify

After recovery:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic city_fares_1m
```

```
city_fares_1m:0:148
```

**Still 148.** Not 296. The source rewound to its checkpointed offsets and the
windows that had already been emitted were not emitted again.

To prove it rather than trust it, check for duplicate window keys:

```bash
docker compose exec kafka bash -c \
  "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
   --topic city_fares_1m --from-beginning --max-messages 148 --timeout-ms 20000" \
  2>/dev/null | sort | uniq -d | wc -l
```

```
0
```

## Record

| Checkpoint | Value |
|---|---|
| Windows emitted before the late event | |
| Windows emitted after the late event | |
| Job state 10 s after `kill` | |
| Job state 60 s after `kill` | |
| TaskManagers / slots once it settles | |
| Windows emitted after recovery | |
| Duplicate window keys | |

Then answer in one line each:

- The late event was dropped silently. What would you have had to build to find out?
- The job could not restart until you returned the TaskManager. What does that imply
  about how much spare capacity a streaming cluster needs?
- For ~40 s the job reported `RUNNING` while a third of its slots were gone. What
  would that do to an alert wired to job state alone?
- Recovery produced no duplicates here. Slide 38 lists three conditions for
  exactly-once — which of them is doing the work, and which one are you relying on
  the sink for?

## Discuss

1. Nothing emitted until the watermark passed the window end. Who pays for the
   out-of-orderness you configure, and in what currency?
2. The final window never fired at all. How would you make a dashboard that is
   correct about "the last minute" given that?
3. You killed a TaskManager and lost nothing. Name the change to this job that would
   make that no longer true.

## Stretch

Add `'sink.delivery-guarantee' = 'exactly-once'` and a transaction timeout to the
sink table, resubmit, and repeat step 4 — then read the sink with
`--isolation-level read_committed`. Measure the read latency you just bought, and
say who is paying it.

Then, separately, add `allowedLateness` to the window and re-send the late event.
The window re-fires and emits a corrected row. Everything downstream must now handle
an update to a row it has already seen — which is a much bigger change than the one
line of SQL suggests.

## Clean up before lab 2.5

Cancel the job so it stops holding all four slots:

```bash
docker compose exec flink-jobmanager ./bin/flink cancel <your-job-id>
rm -f data/late_event.jsonl
```
