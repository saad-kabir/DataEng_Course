#!/usr/bin/env bash
# Downloads every dataset Day 1 needs. Run once, from the labs/ directory.
# Total download: about 55 MB. Takes under a minute on a conference connection.
set -euo pipefail
cd "$(dirname "$0")"

TAXI_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
ZONE_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
OF="https://raw.githubusercontent.com/jpatokal/openflights/master/data"

echo "==> NYC TLC yellow taxi, January 2024 (about 3.0M rows, 48 MB)"
curl -fL --retry 3 -o yellow_tripdata_2024-01.parquet "$TAXI_URL"
curl -fL --retry 3 -o taxi_zone_lookup.csv "$ZONE_URL"

echo "==> OpenFlights airports, airlines, routes (about 2 MB)"
mkdir -p ../import
curl -fL --retry 3 -o ../import/airports.dat "$OF/airports.dat"
curl -fL --retry 3 -o ../import/airlines.dat "$OF/airlines.dat"
curl -fL --retry 3 -o ../import/routes.dat   "$OF/routes.dat"

echo
echo "==> Downloaded:"
ls -lh yellow_tripdata_2024-01.parquet taxi_zone_lookup.csv ../import/*.dat

cat <<'EOF'

Expected, give or take a few KB:
  yellow_tripdata_2024-01.parquet    48M
  taxi_zone_lookup.csv               12K
  ../import/airports.dat            1.1M
  ../import/airlines.dat            388K
  ../import/routes.dat              2.3M

Datasets:
  NYC TLC trip records - public domain, nyc.gov/site/tlc/about/tlc-trip-record-data.page
  OpenFlights          - Open Database License, openflights.org/data
EOF
