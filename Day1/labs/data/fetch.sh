#!/usr/bin/env bash
# Downloads every dataset Days 1 and 2 need. Run once, from the labs/ directory.
# Total download: about 56 MB. Takes under a minute on a conference connection.
set -euo pipefail
cd "$(dirname "$0")"

TAXI_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
ZONE_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONES_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
OF="https://raw.githubusercontent.com/jpatokal/openflights/master/data"

echo "==> NYC TLC yellow taxi, January 2024 (about 3.0M rows, 48 MB)"
curl -fL --retry 3 -o yellow_tripdata_2024-01.parquet "$TAXI_URL"
curl -fL --retry 3 -o taxi_zone_lookup.csv "$ZONE_URL"

echo "==> NYC taxi zone polygons, for the Day 2 PostGIS lab (1 MB shapefile)"
curl -fL --retry 3 -o taxi_zones.zip "$ZONES_URL"
# The archive carries read-only directory permissions, so a plain rm -rf on a
# re-run fails with "Permission denied". Make it writable first.
[ -d taxi_zones ] && chmod -R u+w taxi_zones
rm -rf taxi_zones && mkdir -p taxi_zones
# -o overwrite, -q quiet, -j junk paths: the archive nests everything one level
# deep in taxi_zones/, and shp2pgsql needs the .shp/.dbf/.shx siblings flat.
unzip -oqj taxi_zones.zip -d taxi_zones

echo "==> OpenFlights airports, airlines, routes (about 2 MB)"
mkdir -p ../import
curl -fL --retry 3 -o ../import/airports.dat "$OF/airports.dat"
curl -fL --retry 3 -o ../import/airlines.dat "$OF/airlines.dat"
curl -fL --retry 3 -o ../import/routes.dat   "$OF/routes.dat"

echo
echo "==> Downloaded:"
ls -lh yellow_tripdata_2024-01.parquet taxi_zone_lookup.csv taxi_zones.zip ../import/*.dat

cat <<'EOF'

Expected, give or take a few KB:
  yellow_tripdata_2024-01.parquet    48M
  taxi_zone_lookup.csv               12K
  taxi_zones.zip                    1.0M   (unpacked into taxi_zones/)
  ../import/airports.dat            1.1M
  ../import/airlines.dat            388K
  ../import/routes.dat              2.3M

Datasets:
  NYC TLC trip records - public domain, nyc.gov/site/tlc/about/tlc-trip-record-data.page
  OpenFlights          - Open Database License, openflights.org/data
EOF
