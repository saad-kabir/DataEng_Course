# seed/

Mounted at `/docker-entrypoint-initdb.d` in the postgres container. Files here run
once, in filename order, the first time the `pgdata` volume is created.

`01_schema.sql` creates empty `zones`, `trips` and `trip_events` tables with no
secondary indexes. Lab 0 loads them from the taxi parquet; lab 1.2 indexes them.

To start over from an empty database:

```bash
docker compose down -v postgres      # drops the volume
docker compose up -d postgres
```
