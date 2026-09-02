-- Runs automatically on first postgres start (docker-entrypoint-initdb.d).
-- Deliberately has NO indexes beyond the primary keys. Lab 1.2 adds them.

-- Mounting ./seed at /docker-entrypoint-initdb.d REPLACES that directory, which
-- shadows the image's own 10_postgis.sh. Without this line the extension is
-- installed but never enabled, and Day 2's spatial lab fails on ST_Contains.
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE zones (
  zone_id      int PRIMARY KEY,
  borough      text NOT NULL,
  zone_name    text NOT NULL,
  service_zone text
);

CREATE TABLE trips (
  trip_id       bigserial PRIMARY KEY,
  pickup_ts     timestamptz NOT NULL,
  dropoff_ts    timestamptz NOT NULL,
  pu_zone_id    int NOT NULL,
  do_zone_id    int NOT NULL,
  passengers    smallint,
  distance_mi   numeric(8,2),
  fare_amount   numeric(10,2),
  tip_amount    numeric(10,2),
  total_amount  numeric(10,2),
  payment_type  smallint
);

-- Lab 1.1 writes into this table to make replication lag visible.
CREATE TABLE trip_events (
  event_id   bigserial PRIMARY KEY,
  written_at timestamptz NOT NULL DEFAULT now(),
  payload    text NOT NULL
);

-- Replication role for lab 1.1. pg_basebackup in the pg_replica service uses it.
ALTER ROLE de WITH REPLICATION;
