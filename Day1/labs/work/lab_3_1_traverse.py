import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    mo.md(
        """
        # Lab 3.1 - Model and traverse

        OpenFlights in Neo4j: airports as nodes, routes as relationships.
        Every query here also runs in Neo4j Browser at http://localhost:7474.
        """
    )
    return (mo,)


@app.cell
def _():
    from neo4j import GraphDatabase
    import pandas as pd, time

    driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password"))

    def run(cypher, **params):
        t0 = time.time()
        with driver.session() as s:
            rows = [r.data() for r in s.run(cypher, **params)]
        print(f"{len(rows)} rows in {(time.time()-t0)*1000:.0f} ms")
        return pd.DataFrame(rows)

    driver.verify_connectivity()
    return GraphDatabase, driver, pd, run, time


@app.cell
def _(mo):
    mo.md("""## 1. Constraints before data""")
    return


@app.cell
def _(run):
    run("CREATE CONSTRAINT airport_iata IF NOT EXISTS FOR (a:Airport) REQUIRE a.iata IS UNIQUE")
    run("CREATE CONSTRAINT airline_iata IF NOT EXISTS FOR (l:Airline) REQUIRE l.iata IS UNIQUE")
    run("SHOW CONSTRAINTS YIELD name, labelsOrTypes, properties RETURN name, labelsOrTypes, properties")
    return


@app.cell
def _(mo):
    mo.md("""## 2-4. Load airports, airlines, routes""")
    return


@app.cell
def _(run):
    run(
        """
        LOAD CSV FROM 'file:///airports.dat' AS row
        WITH row WHERE row[4] <> '\\\\N' AND size(row[4]) = 3
        MERGE (a:Airport {iata: row[4]})
          SET a.name = row[1], a.city = row[2], a.country = row[3],
              a.lat = toFloat(row[6]), a.lon = toFloat(row[7])
        """
    )
    run("MATCH (a:Airport) RETURN count(a) AS airports")
    return


@app.cell
def _(run):
    run(
        """
        LOAD CSV FROM 'file:///airlines.dat' AS row
        WITH row WHERE row[3] <> '\\\\N' AND size(row[3]) = 2
        MERGE (l:Airline {iata: row[3]})
          SET l.name = row[1], l.country = row[6], l.active = row[7] = 'Y'
        """
    )
    run("MATCH (l:Airline) RETURN count(l) AS airlines")
    return


@app.cell
def _(run):
    run(
        """
        LOAD CSV FROM 'file:///routes.dat' AS row
        CALL {
          WITH row
          MATCH (src:Airport {iata: row[2]})
          MATCH (dst:Airport {iata: row[4]})
          MERGE (src)-[r:ROUTE {airline: row[0]}]->(dst)
            SET r.stops = toInteger(row[7])
        } IN TRANSACTIONS OF 5000 ROWS
        """
    )
    run("MATCH ()-[r:ROUTE]->() RETURN count(r) AS routes")
    return


@app.cell
def _(mo):
    mo.md("""## 5. One hop - direct from Lisbon""")
    return


@app.cell
def _(run):
    run(
        """
        MATCH (:Airport {iata: $origin})-[:ROUTE]->(dst:Airport)
        RETURN DISTINCT dst.iata AS iata, dst.city AS city, dst.country AS country
        ORDER BY country, city
        """,
        origin="LIS",
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## 6. Two hops - the point of the lab

        Reachable with exactly one connection, and not reachable directly.
        Write the SQL equivalent on paper before you look at the instructor notes.
        """
    )
    return


@app.cell
def _(run):
    run(
        """
        MATCH (o:Airport {iata: $origin})-[:ROUTE]->(:Airport)-[:ROUTE]->(dst:Airport)
        WHERE NOT (o)-[:ROUTE]->(dst) AND dst <> o
        RETURN DISTINCT dst.iata AS iata, dst.city AS city, dst.country AS country
        ORDER BY country, city
        LIMIT 50
        """,
        origin="LIS",
    )
    return


@app.cell
def _(run):
    run(
        """
        MATCH p = shortestPath((:Airport {iata: $a})-[:ROUTE*..6]->(:Airport {iata: $b}))
        RETURN [n IN nodes(p) | n.iata] AS hops, length(p) AS legs
        """,
        a="LIS",
        b="KTM",
    )
    return


@app.cell
def _(mo):
    mo.md("""## 7. Read a plan""")
    return


@app.cell
def _(driver):
    with driver.session() as s:
        res = s.run(
            "PROFILE MATCH (o:Airport {iata:'LIS'})-[:ROUTE]->()-[:ROUTE]->(d:Airport) "
            "RETURN count(DISTINCT d)"
        )
        list(res)
        print(res.consume().profile["args"]["string-representation"])
    return res, s


if __name__ == "__main__":
    app.run()
