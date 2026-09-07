# Lab 2.3 · Partitions, groups, offsets

**Time** 50 minutes · **Slide 31** · **Systems** Kafka

## Objective

Watch a consumer group divide four partitions between its members, starve a fifth
member, survive a death, and then replay the whole topic without the producer being
involved at all.

## Before you start

Stop the lab 2.2 notebook so Spark releases its memory.

You will need **four terminals**, all in `DataEng_Course/Day1/labs`. Open them now
and label them in your head: **A** (commands), **B**, **C**, **D** (consumers).

Every command below is prefixed with the terminal it belongs in.

The Kafka CLI lives at `/opt/kafka/bin/` inside the container and every script ends
in `.sh`. Slide 31 writes them Confluent-style, without the path or the suffix.

## Steps

### 1. Create the topic and load it (10 min)

**A:**

```bash
cd DataEng_Course/Day1/labs
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic trip_events --partitions 4 --replication-factor 1
```

If it already exists from an earlier run, delete it first:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic trip_events
```

Build the event stream - one hour of real January 2024 trips, keyed by `trip_id`:

```bash
docker compose exec marimo uv run python /work/day2_make_events.py
```

```
wrote 2,405 events to /data/trip_events.jsonl
event time from 2024-01-15 08:00:01 to 2024-01-15 08:59:58
```

Produce them:

```bash
docker compose exec kafka bash -c \
  "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
   --topic trip_events --reader-property parse.key=true --reader-property key.separator=: \
   < /data/trip_events.jsonl"
```

Check how the keys landed:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic trip_events
```

```
trip_events:0:605
trip_events:1:635
trip_events:2:557
trip_events:3:608
```

Four partitions, roughly even. That is what keying by `trip_id` buys you. Keying by
`city` would have put ~90% of these rows on one partition, because Manhattan is 90%
of the data — the mistake slide 25 names.

### 2. Two consumers, one group (10 min)

**B:**

```bash
cd DataEng_Course/Day1/labs
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic trip_events \
  --group analytics --from-beginning
```

**C:** the same command again.

Leave both running. **A:**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group analytics
```

Four rows, one per partition. Note the columns: `CURRENT-OFFSET`, `LOG-END-OFFSET`,
`LAG`. **Lag is the only health metric that matters** — it is the one number that
tells you whether the consumer is keeping up, and it needs no knowledge of what the
consumer does.

Now the same group from the other angle:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group analytics --members
```

```
GROUP      CONSUMER-ID                     HOST          CLIENT-ID        #PARTITIONS
analytics  console-consumer-6db2a486-...   /172.18.0.6   console-consumer 2
analytics  console-consumer-7be7efc3-...   /172.18.0.6   console-consumer 2
```

Two members, two partitions each. **Keep both commands to hand.** The default
`--describe` is *partition*-oriented — it has exactly one row per partition, forever,
no matter how many consumers exist. `--members` is *consumer*-oriented. Step 3 turns
on the difference.

### 3. Add a third, then a fifth (10 min)

**D:** the same consumer command again. Re-run the `--members` describe in **A**:
three members now hold 2, 1 and 1 partitions.

Now start two more, so the group has five members for four partitions. Open two more
terminals, or run them detached from **A**:

```bash
docker compose exec -d kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic trip_events --group analytics --from-beginning
```

Run that twice, then look at the group **both ways**.

The default describe:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group analytics
```

Still four rows. Nothing about it has changed, and nothing in it tells you a fifth
consumer exists.

Now with `--members`:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group analytics --members
```

```
GROUP      CONSUMER-ID                     HOST          CLIENT-ID        #PARTITIONS
analytics  console-consumer-b5397847-...   /172.18.0.6   console-consumer 1
analytics  console-consumer-7be7efc3-...   /172.18.0.6   console-consumer 1
analytics  console-consumer-6db2a486-...   /172.18.0.6   console-consumer 1
analytics  console-consumer-d6435605-...   /172.18.0.6   console-consumer 0     <--
analytics  console-consumer-c8949dde-...   /172.18.0.6   console-consumer 1
```

**`#PARTITIONS 0`.** That member will sit idle forever, consuming nothing, and the
partition-oriented view you were watching would never have told you. Partition count
is a hard ceiling on consumer parallelism: you cannot add your way out of lag past
that ceiling, you have to add partitions, and slide 25 explains why doing that later
is expensive.

That two-command habit is the real deliverable of this step. Lag tells you the group
is behind; only the member view tells you that adding consumers will not help.

### 4. Kill a member and watch the rebalance (10 min)

Ctrl-C the consumer in **B** while watching **C** and **D**.

For a moment, *nothing in the group consumes anything*. Every member's partitions are
revoked and reassigned, not just the dead member's. This is the cost slide 26 is
about: a rebalance is a group-wide stop, which is why a slow consumer that misses
`max.poll.interval.ms` triggers a storm rather than an isolated failure.

Re-run the `--members` describe and confirm the four partitions are shared among the
survivors, and that the count of members dropped by one.

### 5. Rewind and replay (10 min)

Stop **every** consumer in the group first — offsets cannot be reset while the group
has active members.

**A:**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group analytics \
  --reset-offsets --to-earliest --topic trip_events --execute
```

```
GROUP           TOPIC           PARTITION  NEW-OFFSET
analytics       trip_events     0          0
analytics       trip_events     1          0
...
```

Start one consumer again. The whole hour replays. **The producer was not involved.**
That is the difference between a log and a queue, and it is what makes replay a
design option rather than a recovery heroic.

## Verify

With every consumer stopped and offsets reset, the describe shows lag equal to the
full topic:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group analytics
```

```
Consumer group 'analytics' has no active members.

GROUP      TOPIC        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
analytics  trip_events  0          0               605             605
analytics  trip_events  1          0               635             635
analytics  trip_events  2          0               557             557
analytics  trip_events  3          0               608             608
```

`CURRENT-OFFSET` 0 on every partition, and `LAG` equal to `LOG-END-OFFSET`. The
"no active members" line is expected — you stopped them all in step 5, and the group
still exists because its committed offsets do.

## Record

Take **partitions per member** and **idle members** from
`--describe --group analytics --members`, not from the default describe.

| Members in group | Partitions per member | Idle members (`#PARTITIONS 0`) |
|---|---|---|
| 2 | | |
| 3 | | |
| 5 | | |

Then answer in one line each:

- How long did consumption pause during the rebalance in step 4?
- Which of the two describe views would have shown you the idle consumer, and what
  does that imply about a lag dashboard built on the other one?
- The topic has 4 partitions. What is the maximum useful group size, and what would
  you have to do to raise it?
- Resetting offsets replayed 2,405 events. Name one thing in your own systems that
  would break if you did that in production, and one that would not care.

## Discuss

1. Lag is the health metric. What does *rising* lag not tell you, and what would you
   look at next?
2. Slide 25 says to over-provision partitions at creation. What does over-provisioning
   cost, and where does that cost show up?
3. You keyed by `trip_id` here. Name a question about this data you could no longer
   answer in order, because of that choice.

## Stretch

Set `cleanup.policy=compact` on a copy of the topic, produce two events with the same
key, and show that only the second survives compaction. Then say in one sentence why
that makes a compacted topic a table rather than a log.

## What this leaves for lab 2.4

Leave `trip_events` populated. Lab 2.4 reads the same topic from Flink. If you
reset the offsets in step 5, that is fine — Flink uses its own consumer group.
