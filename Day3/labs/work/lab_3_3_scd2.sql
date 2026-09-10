-- Lab 3.3 - a type 2 dimension that holds up.
--   docker compose exec -T trino trino -f /work/day3/lab_3_3_scd2.sql
--
-- Builds dim_driver as a type 2 Iceberg table, applies three days of changes
-- (including one edit to an UNTRACKED column), then joins facts to the version that
-- was valid at event time and compares that with the naive current-row join.

CREATE SCHEMA IF NOT EXISTS iceberg.gold;

DROP TABLE IF EXISTS iceberg.gold.dim_driver;
DROP TABLE IF EXISTS iceberg.gold.driver_changes;
DROP TABLE IF EXISTS iceberg.gold.fact_trip;

-- The type 2 dimension. valid_to is EXCLUSIVE (slide 27): a half-open range
-- [valid_from, valid_to) is what stops a fact on a boundary day being counted twice.
CREATE TABLE iceberg.gold.dim_driver (
  driver_id      integer,
  city           varchar,
  licence_class  varchar,
  phone          varchar,          -- deliberately NOT tracked
  tracked_hash   varchar,
  valid_from     timestamp(6),
  valid_to       timestamp(6),
  is_current     boolean
);

-- Day 1: three drivers open their first version.
INSERT INTO iceberg.gold.dim_driver VALUES
  (1, 'Manhattan', 'hack',      '555-0001',
      to_hex(md5(to_utf8('Manhattan' || '|' || 'hack'))),
      TIMESTAMP '2024-01-01 00:00:00', NULL, true),
  (2, 'Brooklyn',  'livery',    '555-0002',
      to_hex(md5(to_utf8('Brooklyn'  || '|' || 'livery'))),
      TIMESTAMP '2024-01-01 00:00:00', NULL, true),
  (3, 'Queens',    'black-car', '555-0003',
      to_hex(md5(to_utf8('Queens'    || '|' || 'black-car'))),
      TIMESTAMP '2024-01-01 00:00:00', NULL, true);

-- Facts either side of the change that is about to happen to driver 1.
CREATE TABLE iceberg.gold.fact_trip (
  trip_id   integer,
  driver_id integer,
  event_ts  timestamp(6),
  revenue   double
);
INSERT INTO iceberg.gold.fact_trip VALUES
  (10, 1, TIMESTAMP '2024-01-02 09:00:00', 100.0),   -- driver 1 still Manhattan
  (11, 1, TIMESTAMP '2024-01-04 09:00:00', 250.0),   -- driver 1 now Brooklyn
  (12, 2, TIMESTAMP '2024-01-02 10:00:00',  40.0),
  (13, 3, TIMESTAMP '2024-01-05 11:00:00',  60.0);

-- The incoming changes. Driver 1 genuinely moves city on the 3rd. Driver 2 only
-- changes a phone number - an UNTRACKED column, which must NOT open a new version.
CREATE TABLE iceberg.gold.driver_changes (
  driver_id     integer,
  city          varchar,
  licence_class varchar,
  phone         varchar,
  changed_at    timestamp(6)
);
INSERT INTO iceberg.gold.driver_changes VALUES
  (1, 'Brooklyn',  'hack',   '555-0001', TIMESTAMP '2024-01-03 00:00:00'),
  (2, 'Brooklyn',  'livery', '555-9999', TIMESTAMP '2024-01-03 00:00:00');
