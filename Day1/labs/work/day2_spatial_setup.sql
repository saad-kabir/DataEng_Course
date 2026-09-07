-- Lab 2.5 - generate the pickup points the spatial join runs against.
-- Run with:
--   docker compose exec -T postgres psql -U de -d rides -f /work/day2_spatial_setup.sql
--
-- WHY THE POINTS ARE GENERATED RATHER THAN LOADED
--
-- The TLC trip extract has carried no latitude or longitude since 2016 - only a
-- PULocationID, which is the zone answer this lab is supposed to compute. So the
-- points are generated INSIDE the real zone polygons, one batch per zone, with the
-- batch size proportional to that zone's real January pickup count.
--
-- That buys two things a random scatter would not:
--   * realistic spatial skew - Manhattan zones get thousands of points, Staten
--     Island a handful, exactly as slide 52 describes
--   * a ground truth - every point remembers the zone it was generated in, so the
--     join has a known correct answer you can check rather than trust.

DROP TABLE IF EXISTS trip_points;

CREATE TABLE trip_points (
  point_id     bigserial PRIMARY KEY,
  true_zone_id int NOT NULL,
  pickup_geom  geometry(Point, 4326) NOT NULL
);

-- ST_GeneratePoints returns one MULTIPOINT per zone; ST_Dump expands it to rows.
-- count(*)/20 puts roughly 110k points on the map: enough that the unindexed join
-- is slow enough to feel, small enough that it still finishes inside the lab.
INSERT INTO trip_points (true_zone_id, pickup_geom)
SELECT z.locationid,
       (ST_Dump(ST_GeneratePoints(z.geom, c.n))).geom
FROM zones_geom z
JOIN (
  SELECT pu_zone_id, GREATEST(1, (count(*) / 20)::int) AS n
  FROM trips
  GROUP BY pu_zone_id
) c ON c.pu_zone_id = z.locationid;

-- The planner guesses badly on geometry (slide 47). Give it what stats it can have.
ANALYZE trip_points;
ANALYZE zones_geom;

SELECT count(*) AS points_generated FROM trip_points;
SELECT count(DISTINCT true_zone_id) AS zones_covered FROM trip_points;
