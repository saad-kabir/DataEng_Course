# Lab 3.1 · Model and traverse

**Time** 50 minutes · **Slide 48** · **System** Neo4j
**Dataset** OpenFlights: 7,700 airports, 6,100 airlines, 67,000 routes
**Notebook** `Day1/labs/work/lab_3_1_traverse.py`

## Objective

Build muscle memory for pattern syntax, and get one visible payoff: a two-hop
question that is three lines of Cypher and a page of SQL.

## Before you start

Lab 0 is complete and `Day1/labs/import/` holds `airports.dat`, `airlines.dat` and
`routes.dat`.

Open Neo4j Browser at **http://localhost:7474** (neo4j / password). The notebook
`lab_3_1_traverse.py` runs the same queries from Python if you prefer.

## The model

```
(:Airport {iata, name, city, country, lat, lon})
   -[:ROUTE {airline, stops}]->
(:Airport)

(:Airline {iata, name, country, active})
```

One node label for places, one for carriers, one relationship type for the flights
between them. Note what is *not* here: no join table, no foreign keys, and the
relationship carries its own properties.

## Steps

### 1. Constraints before data (5 min)

```cypher
CREATE CONSTRAINT airport_iata IF NOT EXISTS
FOR (a:Airport) REQUIRE a.iata IS UNIQUE;

CREATE CONSTRAINT airline_iata IF NOT EXISTS
FOR (l:Airline) REQUIRE l.iata IS UNIQUE;

SHOW CONSTRAINTS;
```

A uniqueness constraint creates a backing index. Without it, every `MATCH` on
`iata` during the load is a full scan, which is what lab 3.2 makes you feel.

### 2. Load airports (10 min)

```cypher
LOAD CSV FROM 'file:///airports.dat' AS row
WITH row WHERE row[4] <> '\\N' AND size(row[4]) = 3
MERGE (a:Airport {iata: row[4]})
  SET a.name    = row[1],
      a.city    = row[2],
      a.country = row[3],
      a.lat     = toFloat(row[6]),
      a.lon     = toFloat(row[7]);
```

```cypher
MATCH (a:Airport) RETURN count(a);
// about 6,000
```

### 3. Load airlines (5 min)

```cypher
LOAD CSV FROM 'file:///airlines.dat' AS row
WITH row WHERE row[3] <> '\\N' AND size(row[3]) = 2
MERGE (l:Airline {iata: row[3]})
  SET l.name = row[1], l.country = row[6], l.active = row[7] = 'Y';
```

### 4. Load routes as relationships (10 min)

```cypher
LOAD CSV FROM 'file:///routes.dat' AS row
CALL {
  WITH row
  MATCH (src:Airport {iata: row[2]})
  MATCH (dst:Airport {iata: row[4]})
  MERGE (src)-[r:ROUTE {airline: row[0]}]->(dst)
    SET r.stops = toInteger(row[7])
} IN TRANSACTIONS OF 5000 ROWS;
```

```cypher
MATCH ()-[r:ROUTE]->() RETURN count(r);
// about 67,000 - one relationship per (source, destination, airline) triple.
// The ~37,000 figure you may expect is the count of distinct city PAIRS; because
// this MERGE keys on airline, each carrier flying a route gets its own edge.
```

### 5. One hop (5 min)

Where can you fly directly from Lisbon?

```cypher
MATCH (:Airport {iata: 'LIS'})-[:ROUTE]->(dst:Airport)
RETURN DISTINCT dst.iata, dst.city, dst.country
ORDER BY dst.country, dst.city;
```

### 6. Two hops, which is the point of the lab (10 min)

Reachable from Lisbon with exactly one connection, and *not* reachable directly:

```cypher
MATCH (lis:Airport {iata: 'LIS'})-[:ROUTE]->(:Airport)-[:ROUTE]->(dst:Airport)
WHERE NOT (lis)-[:ROUTE]->(dst) AND dst <> lis
RETURN DISTINCT dst.iata, dst.city, dst.country
ORDER BY dst.country, dst.city
LIMIT 50;
```

Three lines. Write the SQL equivalent on paper before you look at the answer in the
instructor notes: it needs two self-joins, a NOT EXISTS subquery and a DISTINCT.

Now the shortest path, which has no reasonable SQL form at all:

```cypher
MATCH p = shortestPath(
  (:Airport {iata: 'LIS'})-[:ROUTE*..6]->(:Airport {iata: 'KTM'}))
RETURN [n IN nodes(p) | n.iata] AS hops, length(p) AS legs;
```

### 7. Look at a plan (5 min)

```cypher
PROFILE
MATCH (lis:Airport {iata: 'LIS'})-[:ROUTE]->()-[:ROUTE]->(dst:Airport)
RETURN count(DISTINCT dst);
```

Read `db hits` per operator. The `NodeIndexSeek` on LIS costs a handful; the
expansions cost what they touch. Nothing scans the whole graph.

## Verify

```bash
cd DataEng_Course/Day1/labs
docker compose exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (a:Airport) WITH count(a) AS airports
   MATCH ()-[r:ROUTE]->() RETURN airports, count(r) AS routes;"
```

Expected, approximately:

```
airports | routes
6072     | 66934
```

Within a few hundred either way is fine; OpenFlights is a community dataset and the
filters above drop rows with missing IATA codes. If you want the distinct city-pair
count instead, that is a different question and gives about 37,000:

```cypher
MATCH (s:Airport)-[:ROUTE]->(d:Airport)
RETURN count(DISTINCT [s.iata, d.iata]) AS distinct_city_pairs;
```

## Record

| Query | Rows returned | db hits (PROFILE) | Time (ms) |
|---|---|---|---|
| Direct from LIS | | | |
| Two hops, excluding direct | | | |
| shortestPath LIS to KTM | | | |
| Same two-hop question, written as SQL | (on paper) | — | — |

Legs in the shortest LIS to KTM path: **____**

## Discuss

1. The two-hop query is three lines here. What makes the SQL version long: the join
   count, the DISTINCT, or the NOT EXISTS?
2. `[:ROUTE*..6]` bounds the search at six hops. What happens to the query without a
   bound, and why does the bound belong in the query rather than in a timeout?
3. Which questions about this data would you *still* answer in Postgres?

## Stretch

Add `(:Airline)` to the traversal: find city pairs reachable in two hops on a single
carrier. Then compare against two hops across any carriers, and note how much of the
network disappears.

## On GCP

Neo4j AuraDB runs managed on GCP with the same Cypher and the same GDS library.
Spanner Graph is the native alternative and speaks openCypher for the traversal parts
of this lab.
