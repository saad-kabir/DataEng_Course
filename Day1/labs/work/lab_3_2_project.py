import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    mo.md(
        """
        # Lab 3.2 - Bulk load and project

        Part 1: load the routes twice, without and with a constraint, and time both.
        Part 2: project the graph into memory, run Dijkstra and PageRank.
        """
    )
    return (mo,)


@app.cell
def _():
    from neo4j import GraphDatabase
    import pandas as pd, time

    driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))
    timings = {}

    def run(cypher, label=None, **params):
        t0 = time.time()
        with driver.session() as s:
            rows = [r.data() for r in s.run(cypher, **params)]
        dt = time.time() - t0
        if label:
            timings[label] = round(dt, 1)
        print(f"{len(rows)} rows in {dt*1000:.0f} ms")
        return pd.DataFrame(rows)

    driver.verify_connectivity()
    return GraphDatabase, driver, pd, run, time, timings


@app.cell
def _(mo):
    mo.md("""## Reset to an empty database""")
    return


@app.cell
def _(run):
    run("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS")
    run("DROP CONSTRAINT airport_iata IF EXISTS")
    run("DROP CONSTRAINT airline_iata IF EXISTS")
    run("SHOW CONSTRAINTS YIELD name RETURN name")
    return


@app.cell
def _(mo):
    mo.md("""## Part 1a - load with NO index""")
    return


@app.cell
def _(run):
    LOAD_AIRPORTS = """
    LOAD CSV FROM 'file:///airports.dat' AS row
    WITH row WHERE row[4] <> '\\\\N' AND size(row[4]) = 3
    MERGE (a:Airport {iata: row[4]})
      SET a.name = row[1], a.city = row[2], a.country = row[3]
    """

    LOAD_ROUTES = """
    LOAD CSV FROM 'file:///routes.dat' AS row
    CALL {
      WITH row
      MATCH (src:Airport {iata: row[2]})
      MATCH (dst:Airport {iata: row[4]})
      MERGE (src)-[r:ROUTE {airline: row[0]}]->(dst)
        SET r.stops = toInteger(row[7])
    } IN TRANSACTIONS OF 5000 ROWS
    """

    run(LOAD_AIRPORTS)
    run(LOAD_ROUTES, label="routes_no_index")
    run("MATCH ()-[r:ROUTE]->() RETURN count(r) AS routes")
    return LOAD_AIRPORTS, LOAD_ROUTES


@app.cell
def _(driver):
    with driver.session() as s0:
        r0 = s0.run("EXPLAIN MATCH (a:Airport {iata:'LIS'}) RETURN a")
        list(r0)
        print(r0.consume().plan["args"]["string-representation"])
    return r0, s0


@app.cell
def _(mo):
    mo.md("""## Part 1b - reset, add the constraint, reload""")
    return


@app.cell
def _(LOAD_AIRPORTS, LOAD_ROUTES, run):
    run("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS")
    run("CREATE CONSTRAINT airport_iata IF NOT EXISTS FOR (a:Airport) REQUIRE a.iata IS UNIQUE")
    run(LOAD_AIRPORTS)
    run(LOAD_ROUTES, label="routes_with_index")
    return


@app.cell
def _(driver, timings):
    with driver.session() as s1:
        r1 = s1.run("EXPLAIN MATCH (a:Airport {iata:'LIS'}) RETURN a")
        list(r1)
        print(r1.consume().plan["args"]["string-representation"])

    a, b = timings.get("routes_no_index"), timings.get("routes_with_index")
    if a and b:
        print(f"\nno index: {a}s   with constraint: {b}s   speedup: {a/b:.1f}x")
    return a, b, r1, s1


@app.cell
def _(run):
    run(
        """
        CREATE CONSTRAINT airline_iata IF NOT EXISTS
        FOR (l:Airline) REQUIRE l.iata IS UNIQUE
        """
    )
    run(
        """
        LOAD CSV FROM 'file:///airlines.dat' AS row
        WITH row WHERE row[3] <> '\\\\N' AND size(row[3]) = 2
        MERGE (l:Airline {iata: row[3]})
          SET l.name = row[1], l.country = row[6], l.active = row[7] = 'Y'
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Part 2 - project into memory

        A GDS projection is a separate in-memory object. It does not see writes to the
        stored graph, and it holds memory until you drop it.
        """
    )
    return


@app.cell
def _(run):
    run(
        """
        CALL gds.graph.project('flights', 'Airport', {ROUTE: {orientation: 'NATURAL'}})
        YIELD graphName, nodeCount, relationshipCount
        RETURN graphName, nodeCount, relationshipCount
        """
    )
    return


@app.cell
def _(run):
    run(
        """
        CALL gds.graph.list()
        YIELD graphName, nodeCount, relationshipCount, memoryUsage
        RETURN graphName, nodeCount, relationshipCount, memoryUsage
        """
    )
    return


@app.cell
def _(mo):
    mo.md("""### Shortest path - GDS Dijkstra vs stored-graph Cypher""")
    return


@app.cell
def _(run):
    run(
        """
        MATCH (src:Airport {iata: $a}), (dst:Airport {iata: $b})
        CALL gds.shortestPath.dijkstra.stream('flights', {sourceNode: src, targetNode: dst})
        YIELD totalCost, nodeIds
        RETURN totalCost AS legs, [id IN nodeIds | gds.util.asNode(id).iata] AS route
        """,
        label="gds_dijkstra",
        a="LIS",
        b="KTM",
    )
    return


@app.cell
def _(run):
    run(
        """
        MATCH p = shortestPath((:Airport {iata: $a})-[:ROUTE*..6]->(:Airport {iata: $b}))
        RETURN [n IN nodes(p) | n.iata] AS route, length(p) AS legs
        """,
        label="cypher_shortestpath",
        a="LIS",
        b="KTM",
    )
    return


@app.cell
def _(mo):
    mo.md("""### PageRank against raw degree""")
    return


@app.cell
def _(run):
    pagerank = run(
        """
        CALL gds.pageRank.stream('flights', {maxIterations: 20, dampingFactor: 0.85})
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).iata AS iata,
               gds.util.asNode(nodeId).city AS city,
               round(score, 3) AS pagerank
        ORDER BY pagerank DESC LIMIT 20
        """
    )
    pagerank
    return (pagerank,)


@app.cell
def _(run):
    degree = run(
        """
        MATCH (a:Airport)-[r:ROUTE]->()
        RETURN a.iata AS iata, a.city AS city, count(r) AS out_degree
        ORDER BY out_degree DESC LIMIT 20
        """
    )
    degree
    return (degree,)


@app.cell
def _(degree, mo, pagerank):
    both = pagerank[["iata", "city", "pagerank"]].merge(
        degree[["iata", "out_degree"]], on="iata", how="outer"
    )
    mo.vstack(
        [
            mo.md("### Where the two rankings disagree"),
            mo.ui.table(both.sort_values("pagerank", ascending=False)),
        ]
    )
    return (both,)


@app.cell
def _(mo):
    mo.md("""### Write the scores back, drop the projection""")
    return


@app.cell
def _(run):
    run(
        """
        CALL gds.pageRank.write('flights', {writeProperty: 'pagerank', maxIterations: 20})
        YIELD nodePropertiesWritten, ranIterations
        RETURN nodePropertiesWritten, ranIterations
        """
    )
    run("CALL gds.graph.drop('flights') YIELD graphName RETURN graphName")
    run("CALL gds.graph.list() YIELD graphName RETURN graphName")
    return


@app.cell
def _(mo, timings):
    mo.md(f"### Timings to copy into the record table\n\n```\n{timings}\n```")
    return


if __name__ == "__main__":
    app.run()
