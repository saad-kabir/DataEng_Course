#!/bin/bash
# Lab 1.1 builds pg_replica from this primary with pg_basebackup over the compose
# network. The postgres image's default pg_hba.conf only permits replication from
# localhost, so that connection is rejected with:
#   FATAL: no pg_hba.conf entry for replication connection from host "..."
# Append the entry the standby needs. Runs once, on first init of the pgdata volume.
set -euo pipefail

cat >> "$PGDATA/pg_hba.conf" <<'EOF'

# lab 1.1: streaming replication from the compose network
host    replication    all    all    scram-sha-256
EOF
