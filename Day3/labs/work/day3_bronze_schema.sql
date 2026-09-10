-- Lab 3.1. The bronze landing tables the hourly DAG writes into.
-- Run once:
--   docker compose exec -T postgres psql -U de -d rides -f /work/day3/day3_bronze_schema.sql

CREATE SCHEMA IF NOT EXISTS bronze;

-- Staging is interval-scoped, not global: with max_active_runs=2 two runs are in
-- flight at once, and a single shared staging table would let one run's rows leak
-- into the other's load. This is the same fixed-interval discipline slide 13 is about.
DROP TABLE IF EXISTS bronze.trips_staging;
CREATE TABLE bronze.trips_staging (
  interval_start timestamptz NOT NULL,
  trip_id        bigint      NOT NULL,
  pickup_ts      timestamptz NOT NULL,
  pu_zone_id     int         NOT NULL,
  fare_amount    numeric(10,2)
);
CREATE INDEX ON bronze.trips_staging (interval_start);

-- Two targets, same data, different write pattern. That is the whole lab.
DROP TABLE IF EXISTS bronze.trips_append;
CREATE TABLE bronze.trips_append (
  trip_id     bigint      NOT NULL,
  pickup_ts   timestamptz NOT NULL,
  pu_zone_id  int         NOT NULL,
  fare_amount numeric(10,2),
  loaded_at   timestamptz NOT NULL DEFAULT now()
);

-- The PRIMARY KEY is what makes MERGE possible. Without a real business key there
-- is no idempotent write - which is why slide 7 insists on one.
DROP TABLE IF EXISTS bronze.trips_merge;
CREATE TABLE bronze.trips_merge (
  trip_id     bigint      PRIMARY KEY,
  pickup_ts   timestamptz NOT NULL,
  pu_zone_id  int         NOT NULL,
  fare_amount numeric(10,2),
  loaded_at   timestamptz NOT NULL DEFAULT now()
);

SELECT 'bronze schema ready' AS status;
