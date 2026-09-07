-- Lab 2.4 - windows, lateness, recovery.
-- Run with:  docker compose exec flink-jobmanager ./bin/sql-client.sh -f /work/lab_2_4_windows.sql

-- Checkpoint every 10 s. The cluster default is already 10 s (see docker-compose.yml),
-- but setting it here makes the recovery step in this lab explicit rather than implied.
SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '4';

-- The source. WATERMARK is the promise: "no event more than 5 seconds older than the
-- newest one I have seen will arrive". Everything about when a window fires follows
-- from that one line.
CREATE TABLE trip_events (
  trip_id  STRING,
  city     STRING,
  fare     DOUBLE,
  event_ts TIMESTAMP(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'trip_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-lab-2-4',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- The sink, so the result is a stream you can consume rather than a screen you watch.
CREATE TABLE city_fares_1m (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  city         STRING,
  total_fare   DOUBLE,
  trips        BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'city_fares_1m',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

-- One-minute tumbling windows in EVENT time.
--
-- GROUP BY must carry BOTH window_start AND window_end. Slide 40 abbreviates this to
-- window_start alone; written that way Flink does not recognise a window aggregate at
-- all, plans a regular grouped aggregate that emits updates, and the job fails with
-- "Table sink doesn't support consuming update changes". Try it if you want to see it.
INSERT INTO city_fares_1m
SELECT window_start, window_end, city, SUM(fare), COUNT(*)
FROM TABLE(TUMBLE(TABLE trip_events, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, city;
