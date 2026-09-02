"""Load the taxi parquet into postgres. Used by lab 0 setup.

    uv run python load_postgres.py --rows 2000000

Streams through pyarrow in batches and COPYs into the trips table. About 90
seconds for 2M rows on a laptop.
"""
import argparse, csv, io, os, sys
import pyarrow.parquet as pq
import psycopg

PARQUET = os.environ.get("TAXI_PARQUET", "/data/yellow_tripdata_2024-01.parquet")
ZONES = os.environ.get("TAXI_ZONES", "/data/taxi_zone_lookup.csv")
DSN = os.environ.get(
    "PG_DSN", f"host={os.environ.get('PGHOST', 'localhost')} dbname=rides user=de password=de"
)

COLS = ["pickup_ts", "dropoff_ts", "pu_zone_id", "do_zone_id", "passengers",
        "distance_mi", "fare_amount", "tip_amount", "total_amount", "payment_type"]
SRC = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
       "passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount",
       "payment_type"]


def load_zones(conn):
    with open(ZONES, newline="") as fh, conn.cursor() as cur:
        rows = [
            (int(r["LocationID"]), r["Borough"], r["Zone"], r["service_zone"])
            for r in csv.DictReader(fh)
        ]
        cur.executemany(
            "INSERT INTO zones VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", rows
        )
    print(f"zones: {len(rows)} rows")


def load_trips(conn, limit):
    pf = pq.ParquetFile(PARQUET)
    written = 0
    with conn.cursor() as cur:
        for batch in pf.iter_batches(batch_size=100_000, columns=SRC):
            buf = io.StringIO()
            w = csv.writer(buf)
            for row in zip(*[batch.column(c).to_pylist() for c in SRC]):
                if limit and written >= limit:
                    break
                if row[2] is None or row[3] is None:
                    continue
                w.writerow(["" if v is None else v for v in row])
                written += 1
            buf.seek(0)
            with cur.copy(f"COPY trips ({','.join(COLS)}) FROM STDIN WITH CSV") as cp:
                cp.write(buf.read())
            print(f"  {written:,} rows", end="\r", flush=True)
            if limit and written >= limit:
                break
    print(f"\ntrips: {written:,} rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000, help="0 loads all ~3M")
    a = ap.parse_args()
    with psycopg.connect(DSN, autocommit=True) as conn:
        load_zones(conn)
        load_trips(conn, a.rows)
        with conn.cursor() as cur:
            cur.execute("ANALYZE trips; ANALYZE zones;")
    print("done. run VACUUM ANALYZE if you reload.")
