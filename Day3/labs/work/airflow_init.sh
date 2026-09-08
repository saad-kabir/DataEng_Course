#!/usr/bin/env bash
# One-shot Airflow bootstrap for the Day 3 labs. Runs before the scheduler and API
# server, and is safe to run again.
#
# Airflow keeps its metadata in the SAME Postgres the labs use, in its own `airflow`
# database (deck slide 3). The database has to exist before `airflow db migrate` can
# do anything, and seed/01_schema.sql only runs on a FIRST postgres start - which is
# no help to anyone who already ran Day 1. Hence creating it here, every time.
set -euo pipefail

python - <<'PY'
import psycopg2
conn = psycopg2.connect(host="postgres", user="de", password="de", dbname="postgres")
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
if cur.fetchone():
    print("airflow database already exists")
else:
    cur.execute("CREATE DATABASE airflow OWNER de")
    print("created airflow database")
PY

airflow db migrate
echo "airflow bootstrap complete"
