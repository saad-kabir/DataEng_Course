#!/usr/bin/env python3
"""Build the trip_events stream that labs 2.3 and 2.4 read.

Writes /data/trip_events.jsonl, one `key:json` line per event, ready to pipe into
kafka-console-producer with `--property parse.key=true --property key.separator=:`.

The events are real January 2024 taxi trips from a single hour, carrying their real
pickup timestamps as event time. One hour gives 60 one-minute windows, so lab 2.4
sees windows open and close without waiting for a month of data to stream past.

The file is keyed by trip_id: keying by city would put 90% of the rows on one
partition, which is the mistake slide 25 warns about - and lab 2.3 would then show
three idle consumers instead of a balanced group.
"""

import argparse
import json
import duckdb

TAXI = "/data/yellow_tripdata_2024-01.parquet"
ZONES = "/data/taxi_zone_lookup.csv"
OUT = "/data/trip_events.jsonl"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hour", default="2024-01-15 08", help="event-time hour to extract")
    ap.add_argument("--out", default=OUT)
    args = ap.parse_args()

    rows = duckdb.sql(f"""
        SELECT
            row_number() OVER ()                       AS n,
            t.tpep_pickup_datetime                     AS event_ts,
            z.Borough                                  AS city,
            round(t.total_amount::DOUBLE, 2)           AS fare
        FROM read_parquet('{TAXI}') t
        JOIN read_csv_auto('{ZONES}') z
          ON z.LocationID = t.PULocationID
        WHERE t.tpep_pickup_datetime >= TIMESTAMP '{args.hour}:00:00'
          AND t.tpep_pickup_datetime <  TIMESTAMP '{args.hour}:00:00' + INTERVAL 1 HOUR
          AND t.total_amount BETWEEN 0 AND 500
          AND z.Borough NOT IN ('Unknown', 'N/A')
        ORDER BY t.tpep_pickup_datetime
    """).fetchall()

    with open(args.out, "w") as fh:
        for n, event_ts, city, fare in rows:
            trip_id = f"trip-{n:06d}"
            event = {
                "trip_id": trip_id,
                "city": city,
                "fare": fare,
                # Flink's JSON format parses this into TIMESTAMP(3) directly.
                "event_ts": event_ts.strftime("%Y-%m-%d %H:%M:%S"),
            }
            fh.write(f"{trip_id}:{json.dumps(event)}\n")

    span = (rows[0][1], rows[-1][1]) if rows else (None, None)
    print(f"wrote {len(rows):,} events to {args.out}")
    print(f"event time from {span[0]} to {span[1]}")


if __name__ == "__main__":
    main()
