# Lab 3.2 · Bulk load and project

**Time** 50 minutes · **Slide 51** · **Systems** Neo4j, Graph Data Science
**Notebook** `Day1/labs/work/lab_3_2_project.py`

## Objective

Two halves. First, load the same relationships twice — once without a supporting
index and once with — and feel the difference. Second, project the stored graph into
an in-memory graph and run shortest path and PageRank on it.

The lesson in the first half is that a graph database without the right index is a
graph database doing full scans.

## Before you start

Lab 3.1 is complete. If you want a clean run, the notebook has a reset cell, or:

```cypher
MATCH (n) DETACH DELETE n;
DROP CONSTRAINT airport_iata IF EXISTS;
DROP CONSTRAINT airline_iata IF EXISTS;
```

## Part 1 · The index makes the load (25 min)

### 1. Load airports with no index (10 min)

```cypher
SHOW CONSTRAINTS;   // expect an empty result
```

```cypher
LOAD CSV FROM 'file:///airports.dat' AS row
WITH row WHERE row[4] <> '\\N' AND size(row[4]) = 3
MERGE (a:Airport {iata: row[4]})
  SET a.name = row[1], a.city = row[2], a.country = row[3];
```

Now load routes, which has to `MATCH` two airports per row across 67,000 rows:

```cypher
:auto LOAD CSV FROM 'file:///routes.dat' AS row
CALL {
  WITH row
  MATCH (src:Airport {iata: row[2]})
  MATCH (dst:Airport {iata: row[4]})
  MERGE (src)-[r:ROUTE {airline: row[0]}]->(dst)
} IN TRANSACTIONS OF 5000 ROWS;
```

**Time this.** Expect minutes. Each `MATCH` is a label scan over every airport node.

Confirm what the planner did:

```cypher
EXPLAIN MATCH (a:Airport {iata: 'LIS'}) RETURN a;
// NodeByLabelScan  ->  Filter
```

### 2. Reset, index, reload (10 min)

```cypher
MATCH (n) DETACH DELETE n;

CREATE CONSTRAINT airport_iata IF NOT EXISTS
FOR (a:Airport) REQUIRE a.iata IS UNIQUE;
```

Re-run both loads from step 1 unchanged, and time the routes load again.

```cypher
EXPLAIN MATCH (a:Airport {iata: 'LIS'}) RETURN a;
// NodeUniqueIndexSeek
```

Same Cypher, same data, one plan operator different.

### 3. Load the airlines and connect them (5 min)

```cypher
CREATE CONSTRAINT airline_iata IF NOT EXISTS
FOR (l:Airline) REQUIRE l.iata IS UNIQUE;

LOAD CSV FROM 'file:///airlines.dat' AS row
WITH row WHERE row[3] <> '\\N' AND size(row[3]) = 2
MERGE (l:Airline {iata: row[3]})
  SET l.name = row[1], l.country = row[6], l.active = row[7] = 'Y';
```

## Part 2 · Project and compute (25 min)

A GDS projection copies the topology you name into memory. It is a separate object
with its own lifecycle, and it does not see writes to the stored graph.

### 4. Project the route network (5 min)

```cypher
CALL gds.graph.project(
  'flights',
  'Airport',
  { ROUTE: { orientation: 'NATURAL' } }
)
YIELD graphName, nodeCount, relationshipCount;
```

```cypher
CALL gds.graph.list() YIELD graphName, nodeCount, relationshipCount, memoryUsage;
```

Note the memory figure. That is the cost of the projection, held until you drop it.

### 5. Shortest path (10 min)

```cypher
MATCH (src:Airport {iata: 'LIS'}), (dst:Airport {iata: 'KTM'})
CALL gds.shortestPath.dijkstra.stream('flights', {
  sourceNode: src,
  targetNode: dst
})
YIELD totalCost, nodeIds
RETURN totalCost AS legs,
       [id IN nodeIds | gds.util.asNode(id).iata] AS route;
```

Compare against the `shortestPath()` you ran in lab 3.1 on the stored graph. Same
answer, different execution: Cypher expanded the stored relationships, GDS walked an
in-memory adjacency structure.

Time both and record the difference.

### 6. PageRank (10 min)

Which airports matter to the network, rather than which have the most routes?

```cypher
CALL gds.pageRank.stream('flights', { maxIterations: 20, dampingFactor: 0.85 })
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).iata  AS iata,
       gds.util.asNode(nodeId).city  AS city,
       round(score, 3)               AS pagerank
ORDER BY pagerank DESC LIMIT 20;
```

Now compare with the naive answer, raw degree:

```cypher
MATCH (a:Airport)-[r:ROUTE]->()
RETURN a.iata, a.city, count(r) AS out_degree
ORDER BY out_degree DESC LIMIT 20;
```

The two lists overlap heavily at the top and diverge in the middle. PageRank rewards
being connected to *well-connected* airports, so a small hub that feeds three major
ones outranks a busy regional airport that only serves other regional airports.

### 7. Write the score back and drop the projection (5 min)

```cypher
CALL gds.pageRank.write('flights', {
  writeProperty: 'pagerank', maxIterations: 20
}) YIELD nodePropertiesWritten, ranIterations;

CALL gds.graph.drop('flights') YIELD graphName;
CALL gds.graph.list();   // empty
```

The scores persist on the nodes. The projection does not.

## Verify

```bash
cd DataEng_Course/Day1/labs
docker compose exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (a:Airport) WHERE a.pagerank IS NOT NULL
   RETURN count(a) AS scored, round(max(a.pagerank),2) AS top;"
```

Expected: several thousand scored nodes and a top score in the low tens. The
highest-ranked airports should be recognisable global hubs.

## Record

| Measurement | Yours |
|---|---|
| Routes load, no index | |
| Routes load, with constraint | |
| Speedup | ____ × |
| Projection memory usage | |
| Nodes / relationships in the projection | |
| `shortestPath()` on the stored graph (ms) | |
| `gds.shortestPath.dijkstra` on the projection (ms) | |
| Top 3 by PageRank | |
| Top 3 by out-degree | |

## Discuss

1. The unindexed load was slower by a large factor, and the Cypher was identical. What
   is the equivalent mistake in the Postgres lab you ran this morning?
2. A projection is a point-in-time copy. What breaks if the stored graph changes while
   an algorithm is running against the projection?
3. PageRank and degree disagree in the middle of the ranking. Which one would you put
   in front of a network planning team, and how would you explain the difference?

## Stretch

Project a weighted graph using `stops` as the relationship weight and re-run Dijkstra.
Then run `gds.louvain` on the unweighted projection and look at which countries fall
into the same community. Communities that cross borders usually trace an alliance.

## On GCP

Neo4j AuraDS is the managed GDS offering and runs on GCP with these exact procedure
names. For graphs that outgrow a single machine, the same PageRank runs on Dataproc
with Spark GraphFrames, with a very different cost profile.
