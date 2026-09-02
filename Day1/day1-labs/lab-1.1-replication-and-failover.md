# Lab 1.1 · Replication and failover

**Time** 50 minutes · **Slide 14** · **Systems** Postgres primary and streaming replica

## Objective

Make replication lag physical. Write to the primary, read the same query from both
nodes, and watch the replica fall behind and catch up. Then pause the replica and
observe that nothing in the application layer notices.

## Before you start

Lab 0 is complete and `trips` holds 2 million rows. Every command below is run from
`DataEng_Course/Day1/labs`, the directory holding `docker-compose.yml`.

## Steps

### 1. Start the replica (5 min)

`pg_replica` sits behind the `replica` profile, so `docker compose up -d` never
starts it by accident. Naming it explicitly enables the profile for that command:

```bash
cd DataEng_Course/Day1/labs
docker compose up -d pg_replica
docker compose logs -f pg_replica
```

On first start the container runs `pg_basebackup` against the primary, copies the
whole data directory, and starts in standby mode. Watch for
`entering standby mode` then `database system is ready to accept read only connections`.
Ctrl-C out of the logs.

> Note: It might take some time to see the logs.

### 2. Confirm the roles (5 min)

```bash
# primary: f = false, this node accepts writes
docker compose exec postgres psql -U de -d rides -c "SELECT pg_is_in_recovery();"

# replica: t = true, read only
docker compose exec pg_replica psql -U de -d rides -c "SELECT pg_is_in_recovery();"
```

Prove the replica refuses writes:

```bash
docker compose exec pg_replica psql -U de -d rides \
  -c "INSERT INTO trip_events (payload) VALUES ('nope');"
# ERROR:  cannot execute INSERT in a read-only transaction
```



### 3. Read the replication state (5 min)

```bash
docker compose exec postgres psql -U de -d rides -x -c "
SELECT client_addr,
       state,
       sent_lsn,
       replay_lsn,
       pg_wal_lsn_diff(sent_lsn, replay_lsn) AS replay_lag_bytes,
       replay_lag
  FROM pg_stat_replication;"
```

With no write traffic, `replay_lag_bytes` is 0 and `replay_lag` is null. Note that
Postgres reports the lag in **bytes of WAL**, not seconds, until it has a timing
sample to work from.

### 4. Generate write pressure (10 min)

Open a second terminal and leave this running:

```bash
docker compose exec postgres psql -U de -d rides -c "
INSERT INTO trip_events (payload)
SELECT repeat(md5(random()::text), 40)
  FROM generate_series(1, 2000000);"
```

While it runs, poll both nodes from the first terminal:

```bash
for i in $(seq 1 20); do
  P=$(docker compose exec -T postgres  psql -U de -d rides -tAc "SELECT count(*) FROM trip_events;")
  R=$(docker compose exec -T pg_replica psql -U de -d rides -tAc "SELECT count(*) FROM trip_events;")
  echo "$(date +%T)  primary=$P  replica=$R  behind=$((P-R))"
  sleep 1
done
```

Watch the lag column in a third view:

```bash
watch -n1 'docker compose exec -T postgres psql -U de -d rides -tAc \
  "SELECT pg_wal_lsn_diff(sent_lsn, replay_lsn), replay_lag FROM pg_stat_replication;"'
```



### 5. Pause the replica (10 min)

While the insert is still running:

```bash
docker compose pause pg_replica
```

Keep polling the primary. It continues accepting writes at full speed. Now check
what the primary is holding on to:

```bash
docker compose exec postgres psql -U de -d rides -c "
SELECT slot_name, active, restart_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
  FROM pg_replication_slots;"

# $PGDATA, not a hardcoded path: on the PG18 image the data directory is
# /var/lib/postgresql/18/docker, so the old /var/lib/postgresql/data does not exist.
docker compose exec postgres bash -c 'du -sh "$PGDATA/pg_wal"'
```

Then resume and watch it catch up:

```bash
docker compose unpause pg_replica
```



### 6. Promote the replica (10 min)

```bash
docker compose exec pg_replica psql -U de -d rides -c "SELECT pg_promote();"
docker compose exec pg_replica psql -U de -d rides -c "SELECT pg_is_in_recovery();"
# f  -- it is now a primary
```

Write to it, and note that you now have two nodes that both accept writes and no
longer agree. That is the split brain a real failover procedure exists to prevent.

## Verify

```bash
docker compose exec pg_replica psql -U de -d rides -tAc \
  "SELECT pg_is_in_recovery(), count(*) FROM trip_events;"
```

Expected: `f|2000000` — recovery finished, and the replica replayed every row
written before the promotion.

## Record


| Measurement                         | Where it came from     | Yours |
| ----------------------------------- | ---------------------- | ----- |
| Peak rows behind, during the insert | step 4 poll loop       |       |
| Peak `replay_lag_bytes`             | `pg_stat_replication`  |       |
| `replay_lag` in seconds, at peak    | `pg_stat_replication`  |       |
| WAL retained after 60 s paused      | `pg_replication_slots` |       |
| `pg_wal` directory size, paused     | `du -sh`               |       |
| Seconds to catch up after unpause   | your own timing        |       |




## Discuss

1. The application never saw an error while the replica was hours behind. What in
  your own stack would have caught it?
2. The replication slot kept WAL for a replica that was not consuming it. What
  happens to the primary if nobody notices for a week?
3. Lag is measured in bytes. What has to be true for you to convert that to
  "how stale is the answer this dashboard just gave me"?



## Stretch

Set `synchronous_commit = on` with `synchronous_standby_names = '*'` on the primary,
restart it, and re-run step 4 with the replica paused. Commits now block. Measure how
long the primary stays unavailable, and decide which of your own tables would accept
that trade.

## On GCP

Cloud SQL publishes the same signal as `replica_lag` in Cloud Monitoring, and its
managed failover does the promotion and the DNS swap in one step. AlloyDB ships
storage blocks rather than replaying WAL, so its read pool lags far less under the
write pressure you just generated.
