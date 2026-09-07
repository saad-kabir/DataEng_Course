# Lab 2.5 · Point in polygon, three ways

**Time** 50 minutes · **Slide 48** · **Systems** PostGIS
**SQL** `Day1/labs/work/day2_spatial_setup.sql`

> The Day 2 deck labels this **Lab 3.1**. It is lab **2.5** here, because Day 3's own
> labs are numbered 3.1 to 3.4 and the identifier has to mean one thing across the
> week. Same lab, same slide.

## Objective

Assign ~99,500 pickup points to 263 zone polygons three times — with no index, with
a GiST index, and against a subdivided copy of the zones — and read the cost of each
out of `EXPLAIN ANALYZE`.

## Before you start

Postgres is up and PostGIS is enabled:

```bash
cd DataEng_Course/Day1/labs
docker compose exec postgres psql -U de -d rides -c "SELECT postgis_version();"
```

```
 3.6 USE_GEOS=1 USE_PROJ=1 USE_STATS=1
```

The zone polygons are on disk. If `data/taxi_zones/` is missing, run `./data/fetch.sh`.

```bash
ls data/taxi_zones/
```

```
taxi_zones.cpg  taxi_zones.dbf  taxi_zones.prj  taxi_zones.shp  taxi_zones.shx
```

Free some memory first — nothing else today needs Spark or Flink:

```bash
docker compose stop marimo flink-jobmanager flink-taskmanager-1 flink-taskmanager-2
```

## Steps

### 1. Load the zone polygons (10 min)

`shp2pgsql` ships inside the PostGIS image, so this is one pipe with no extra tools:

```bash
docker compose exec postgres bash -c \
  "shp2pgsql -s 2263:4326 -D /data/taxi_zones/taxi_zones.shp zones_geom \
   | psql -U de -d rides -q"
```

`-s 2263:4326` is the important flag. The shapefile is in **EPSG:2263**, New York
State Plane in *feet* — read `data/taxi_zones/taxi_zones.prj` and you will see
`StatePlane_New_York_Long_Island..._Feet`. Storing it as 4326 without transforming
would put these polygons in the Gulf of Guinea, and nothing would error. Slide 43:
mixing CRS gives silently wrong answers rather than a failure.

Note there is **no `-I` flag**. That would build the GiST index, and step 3 is about
what life is like without one.

Check the load, and that the coordinates really are New York:

```bash
docker compose exec postgres psql -U de -d rides -c \
 "SELECT count(*) AS zones, ST_SRID(geom) AS srid FROM zones_geom GROUP BY 2;" -c \
 "SELECT round(ST_XMin(ST_Extent(geom))::numeric,2) AS min_lon,
         round(ST_YMax(ST_Extent(geom))::numeric,2) AS max_lat FROM zones_geom;"
```

```
 zones | srid
-------+------
   263 | 4326

 min_lon | max_lat
---------+---------
  -74.26 |   40.92
```

263, not the 265 in yesterday's lookup CSV: zones 264 and 265 are "Unknown" and
"Outside NYC" and have no geometry.

### 2. Generate the pickup points (5 min)

```bash
docker compose exec -T postgres psql -U de -d rides -f /work/day2_spatial_setup.sql
```

```
 points_generated
------------------
            99498

 zones_covered
---------------
           253
```

That count is derived from the row count in `trips`, so it will differ if you loaded
a different number in lab 0. 99,498 corresponds to the standard 2,000,000 rows.

Read the header of `day2_spatial_setup.sql` before moving on — it explains why the
points are generated rather than loaded, and what that buys.

### 3. Phase 1 — no index (10 min)

```bash
docker compose exec postgres psql -U de -d rides -c "
EXPLAIN (ANALYZE, BUFFERS)
SELECT z.locationid, count(*)
  FROM trip_points t JOIN zones_geom z
    ON ST_Contains(z.geom, t.pickup_geom)
 GROUP BY z.locationid;"
```

Expect **10 to 20 seconds**. The spread is wide because Postgres may or may not
choose a parallel plan for this join depending on what else is running - if you left
Spark or Flink up, expect the slow end. In the plan, find three things:

```
Nested Loop  (cost=0.00..327428780.94 rows=54076 ...) (actual ... rows=99498 ...)
  ->  Seq Scan on zones_geom z  ... rows=263
```

- a **Nested Loop** with a **Seq Scan on the inner side** — every point is tested
  against every polygon
- the row **estimate versus actual**: the planner guessed ~54,000 and got 99,498.
  Your estimate will differ by a few hundred — it comes from sampled statistics.
  Slide 47 — the planner guesses badly on geometry, and it is not close
- the cost estimate, 327 million, against a query that takes seconds

### 4. Phase 2 — add the GiST index (10 min)

```bash
docker compose exec postgres psql -U de -d rides -c \
 "CREATE INDEX zones_geom_geom_idx ON zones_geom USING GIST (geom);" -c \
 "ANALYZE zones_geom;"
```

`ANALYZE` is not optional — slide 46 puts it right after the `CREATE INDEX` for a
reason. Re-run the identical query from step 3.

Expect roughly **1.5 seconds**, a 10× to 12× improvement. The plan changes shape:

```
Index Scan using zones_geom_geom_idx on zones_geom z ... rows=1 loops=99498
  Rows Removed by Filter: 1
```

`Rows Removed by Filter` is the fan-out that phase 1 of the join could not avoid:
the bounding box said "maybe", the exact `ST_Contains` said "no". That number is the
cost of boxes being a bad approximation of a shape.

### 5. Phase 3 — subdivide the polygons (10 min)

```bash
docker compose exec postgres psql -U de -d rides -c "
CREATE TABLE zones_sub AS
  SELECT locationid, ST_Subdivide(geom, 512) AS geom FROM zones_geom;
CREATE INDEX zones_sub_geom_idx ON zones_sub USING GIST (geom);
ANALYZE zones_sub;"
```

See what it did to the geometry:

```bash
docker compose exec postgres psql -U de -d rides -c "
SELECT (SELECT count(*) FROM zones_geom) AS zones,
       (SELECT count(*) FROM zones_sub)  AS pieces,
       (SELECT round(avg(ST_NPoints(geom))) FROM zones_geom) AS avg_vertices_zone,
       (SELECT round(avg(ST_NPoints(geom))) FROM zones_sub)  AS avg_vertices_piece;"
```

```
 zones | pieces | avg_vertices_zone | avg_vertices_piece
-------+--------+-------------------+--------------------
   263 |    616 |               373 |                161
```

Then run the same query against `zones_sub`:

```bash
docker compose exec postgres psql -U de -d rides -c "
EXPLAIN (ANALYZE, BUFFERS)
SELECT z.locationid, count(*)
  FROM trip_points t JOIN zones_sub z
    ON ST_Contains(z.geom, t.pickup_geom)
 GROUP BY z.locationid;"
```

Expect roughly **1.2 seconds**. A further 1.2× to 1.4× — real, but far smaller than
the index gave you. Same answer, less CPU, because each piece has a tighter bounding
box and less than half the vertices, so both phases of the join get cheaper at once.

Note the shape of the result: the index was worth ~11×, subdividing another ~1.25×.
When you have one optimisation to spend, that ratio is the whole argument.

### 6. Check the answer is actually right (5 min)

Every point remembers the zone it was generated in, so the join has a ground truth:

```bash
docker compose exec postgres psql -U de -d rides -c "
WITH j AS (
  SELECT t.true_zone_id AS truth, z.locationid AS joined_zone
  FROM trip_points t JOIN zones_sub z ON ST_Contains(z.geom, t.pickup_geom))
SELECT count(*) AS matched,
       count(*) FILTER (WHERE truth = joined_zone) AS agrees,
       count(*) FILTER (WHERE truth <> joined_zone) AS disagrees FROM j;"
```

## Verify

```
 matched | agrees | disagrees
---------+--------+-----------
   99498 |  99498 |         0
```

Every point matched exactly one zone, and it was the right one. The three phases
differ by a factor of about fourteen end to end and agree on the answer exactly —
which is the whole argument for indexing before optimising anything else.

## Record

| Phase | Plan shape | Execution time | Rows removed by filter |
|---|---|---|---|
| 1 no index | Nested Loop + Seq Scan | | n/a |
| 2 GiST index | | | |
| 3 subdivided | | | |

Then answer in one line each:

- Phase 1 to phase 2: **____ ×** faster
- Phase 2 to phase 3: **____ ×** faster
- Row estimate versus actual in phase 1: **____** vs **____**
- Phase 3 stores 616 rows instead of 263. When is that a bad trade?

## Discuss

1. The GiST index gave roughly 11× and subdividing gave roughly another 1.25×. If you
   could only do one, which, and what about your data would change the answer?
2. `Rows Removed by Filter` was the bounding-box fan-out. Which shape of polygon
   makes that number explode, and what would you do about that one polygon?
3. Slide 47 lists simplify, subdivide, and grid-approximate as the three fixes in
   order. You did the middle one. What would you have to know before reaching for
   the third?

## Stretch

Add a simplified, indexed second geometry column
(`ST_Simplify(geom, 0.0001)`) and join against that instead, then compare both the
runtime and the number of points that now land in a different zone. You have just
traded correctness for speed — quantify the trade before you decide whether it was
worth it.

## Restart what you stopped

```bash
cd DataEng_Course/Day1/labs
docker compose up -d
```
