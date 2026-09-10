# Lab 3.2 · CDC into bronze, tested

**Time** 50 minutes · **Slide 20** · **Systems** Debezium, Kafka Connect, Postgres
**Config** `Day3/labs/work/day3_debezium_connector.json`

## Objective

Stream a Postgres table's write-ahead log into Kafka, apply an update and a hard
delete, and see what each one looks like on the other side — including what happens
to the deleted row if your merge forgets about it.

## Before you start

Kafka Connect is healthy:

```bash
cd DataEng_Course/Day1/labs
curl -s http://localhost:8083/connectors
```

```
[]
```

Postgres must be running `wal_level=logical`. Debezium decodes the WAL logically and
will not start otherwise:

```bash
docker compose exec postgres psql -U de -d rides -tAc "SHOW wal_level;"
```

```
logical
```

Create the source table:

```bash
docker compose exec -T postgres psql -U de -d rides -f /work/day3/day3_cdc_source.sql
```

```
 seeded_rows
-------------
         200
```

200 rows, not the two million in `trips`, on purpose: Debezium's initial snapshot
publishes every existing row before a single change event appears, and this lab is
about the change events.

## Steps

### 1. Read the connector config (10 min)

Open `Day3/labs/work/day3_debezium_connector.json`. Four settings matter more than
the rest.

| Setting | Why |
|---|---|
| `plugin.name: pgoutput` | Postgres's built-in logical decoding plugin — no extension to install |
| `slot.name` / `publication.name` | the replication slot and publication Debezium owns. **The slot is the operational risk from slide 6** |
| `decimal.handling.mode: double` | without it, `numeric` columns arrive base64-encoded and `fare` is unreadable |
| `topic.creation.default.*` | the broker has auto-create **off** (Day 2), so Connect has to be told how to make its own topics |

The source table also carries `REPLICA IDENTITY FULL`. Without it a delete event
carries only the primary key and an update carries only what changed — so bronze
could not be an audit trail. It costs WAL volume. That is the trade.

### 2. Register the connector (5 min)

```bash
curl -s -X POST -H "Content-Type: application/json" \
  --data @../../Day3/labs/work/day3_debezium_connector.json \
  http://localhost:8083/connectors | head -c 120; echo

curl -s http://localhost:8083/connectors/rides-cdc/status
```

Wait for `"state":"RUNNING"` on both the connector and its task. The initial snapshot
publishes all 200 rows:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic cdc.public.cdc_trips
```

```
cdc.public.cdc_trips:0:200
```

### 3. Apply the deck's update and delete (10 min)

```bash
docker compose exec postgres psql -U de -d rides -c "
UPDATE cdc_trips SET fare = 44.10 WHERE trip_id = 812;
DELETE FROM cdc_trips WHERE trip_id = 813;"
```

Two more events appear. Read the last two:

```bash
docker compose exec kafka bash -c \
 "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic cdc.public.cdc_trips --from-beginning --max-messages 202 --timeout-ms 30000" \
 2>/dev/null | tail -2
```

The shapes to look for:

```
op=u   before={trip_id: 812, fare: 22.0}   after={trip_id: 812, fare: 44.1}
op=d   before={trip_id: 813, fare: 23.0}   after=null
```

`op=u` carries **both** images, so bronze keeps every version of row 812 — the audit
trail you will want later. `op=d` carries the before image and a null after. A hard
delete is a first-class event here, which is exactly what query-based ingestion
cannot give you (slide 5).

### 4. Merge the change stream into silver (15 min)

Build silver from the change events, newest event per key wins:

```bash
docker compose exec -T postgres psql -U de -d rides <<'SQL'
DROP TABLE IF EXISTS silver_trips;
CREATE TABLE silver_trips AS SELECT * FROM cdc_trips WITH NO DATA;
ALTER TABLE silver_trips ADD PRIMARY KEY (trip_id);
INSERT INTO silver_trips SELECT * FROM cdc_trips;
SELECT count(*) AS silver_rows FROM silver_trips;
SQL
```

Now ask the question the lab is really about. Row 813 was deleted at source. Is it
gone from silver?

```bash
docker compose exec postgres psql -U de -d rides -c \
 "SELECT count(*) AS row_813_in_silver FROM silver_trips WHERE trip_id = 813;"
```

If your merge has no `WHEN MATCHED AND op = 'd' THEN DELETE` clause, **row 813 lives
in silver forever** and no test notices unless you wrote one. Write that test:

```bash
docker compose exec postgres psql -U de -d rides -c "
SELECT 'orphaned_deletes' AS test,
       count(*) AS failures,
       CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
  FROM silver_trips s
  WHERE NOT EXISTS (SELECT 1 FROM cdc_trips c WHERE c.trip_id = s.trip_id);"
```

### 5. Make the slot risk concrete (10 min)

Pause the connector and note what the slot is holding:

```bash
curl -s -X PUT http://localhost:8083/connectors/rides-cdc/pause

docker compose exec postgres psql -U de -d rides -c "
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
  FROM pg_replication_slots WHERE slot_name = 'rides_cdc_slot';"
```

Now write to `trips` — a table this connector does **not** capture:

```bash
docker compose exec postgres psql -U de -d rides -c \
 "UPDATE trips SET fare_amount = fare_amount WHERE trip_id <= 200000;"
```

Check again:

```
 active | retained_wal
--------+--------------
 t      | 102 MB
```

**102 MB, from a table the connector is not even subscribed to.** A replication slot
holds the WAL of the *entire database* from its `restart_lsn` forward, because
Postgres cannot know which future records the consumer will need until it has read
them. One paused connector on one small table can fill the disk with WAL generated by
work that has nothing to do with it.

That is slide 6 made concrete, and it is why the alert is on **slot lag in bytes**,
not on connector uptime. The connector here is `RUNNING` and `active = t` the entire
time.

Resume it:

```bash
curl -s -X PUT http://localhost:8083/connectors/rides-cdc/resume
```

The retained figure falls once the connector consumes the backlog **and flushes its
offsets** — which is periodic, not instant. Do not expect the number to drop the
moment you resume; watch it over a minute. On an idle database it can even tick up
first, because `pg_current_wal_lsn()` keeps moving while `restart_lsn` waits for the
next offset commit.

## Verify

```bash
curl -s http://localhost:8083/connectors/rides-cdc/status | head -c 200; echo
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic cdc.public.cdc_trips
docker compose exec postgres psql -U de -d rides -tAc \
 "SELECT active FROM pg_replication_slots WHERE slot_name='rides_cdc_slot';"
```

Connector `RUNNING`, topic offset **202 or more**, slot `active` = `t`.

## Record

| Check | Value |
|---|---|
| Rows in the initial snapshot | |
| Topic offset after update + delete | |
| `op` of the last two events | |
| Row 813 present in silver? | |
| Retained WAL after the uncaptured 200k-row update | |
| Retained WAL one minute after resume | |

Then answer in one line each:

- What would query-based ingestion on `updated_at` have done with row 813?
- Which single connector setting made `fare` readable, and what was it before?
- Your alert says "connector up". What incident does that alert miss entirely?

## Discuss

1. Bronze keeps every version of row 812. What does that let you answer that silver
   cannot, and what does it cost you every day?
2. The uniqueness test fails if you merge without ordering by log position. Why does
   the last event have to win, and what breaks if two events share a timestamp?
3. `REPLICA IDENTITY FULL` puts the whole before image in the WAL. When would you
   refuse to pay that, and what do you lose?

## Stretch

Reset the connector completely and re-snapshot from scratch. The order matters, and
getting it wrong is instructive:

```bash
curl -s -X PUT    http://localhost:8083/connectors/rides-cdc/stop
curl -s -X DELETE http://localhost:8083/connectors/rides-cdc/offsets
curl -s -X DELETE http://localhost:8083/connectors/rides-cdc

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic cdc.public.cdc_trips
docker compose exec postgres psql -U de -d rides -tAc \
  "SELECT pg_drop_replication_slot('rides_cdc_slot');"
```

Then re-register and confirm you get a fresh 200-row snapshot.

**Try it in the wrong order once.** Delete the connector *first*, then the topic and
the slot, then re-register with the same name. Connect stores its offsets under the
connector **name**, and `DELETE /connectors/{name}/offsets` only works while the
connector still exists — so the new connector inherits the old offsets, believes it
has already snapshotted, and sits there `RUNNING` and publishing nothing. A green
connector with an empty topic is one of the more confusing states you will meet in
production, and you have now seen it deliberately.

## What this leaves for labs 3.3 and 3.4

Nothing. Leave the connector running if you like; Iceberg does not touch it.
