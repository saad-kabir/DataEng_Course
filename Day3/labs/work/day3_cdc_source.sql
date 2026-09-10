-- Lab 3.2. A small, self-contained CDC source table.
--   docker compose exec -T postgres psql -U de -d rides -f /work/day3/day3_cdc_source.sql
--
-- Deliberately NOT the 2-million-row `trips` table: Debezium's initial snapshot would
-- publish all two million rows before a single change event appeared, and the lab is
-- about the change events. A few hundred rows snapshot instantly.

DROP TABLE IF EXISTS cdc_trips;
CREATE TABLE cdc_trips (
  trip_id     bigint PRIMARY KEY,
  pu_zone_id  int          NOT NULL,
  fare        numeric(10,2) NOT NULL,
  status      text          NOT NULL DEFAULT 'complete',
  updated_at  timestamptz   NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL puts the BEFORE image of every row in the WAL. Without it a
-- delete event carries only the primary key, and an update carries only what changed
-- - so bronze cannot be a real audit trail. It costs WAL volume; that is the trade.
ALTER TABLE cdc_trips REPLICA IDENTITY FULL;

INSERT INTO cdc_trips (trip_id, pu_zone_id, fare)
SELECT g, 100 + (g % 60), round((10 + (g % 40))::numeric, 2)
FROM generate_series(801, 1000) AS g;

SELECT count(*) AS seeded_rows FROM cdc_trips;
