# Running Day 1

## Schedule

| Time | Block |
|---|---|
| 09:00 | Slides 1-13, block 1 lecture |
| 10:00 | **Lab 1.1** replication and failover |
| 10:50 | Break |
| 11:05 | Slides 15-20 |
| 11:40 | **Lab 1.2** indexes and plans |
| 12:30 | Lunch |
| 13:30 | Slides 22-29, block 2 |
| 14:10 | **Lab 2.1** object store and planning |
| 15:00 | Break |
| 15:15 | Slides 31-38 |
| 15:50 | **Lab 2.2** layout experiment |
| 16:40 | Break |
| 16:50 | Slides 41-47, block 3 |
| 17:20 | **Lab 3.1** model and traverse |
| 18:10 | **Lab 3.2** bulk load and project (or push to Day 2 morning) |

Lab 0 goes out the evening before. Say so explicitly and give the download size —
people on hotel wifi need the warning.

## Send before the day

1. The `Day1/labs/` and `Day1/day1-labs/` folders (minus `instructor/`), or the repo URL
2. `lab-0-setup.md`
3. One line: "from `DataEng_Course/Day1/labs`, run `./data/fetch.sh` then
   `docker compose up -d` before you arrive — it pulls about 4 GB of images"

## Demo live rather than making them run it

- **Lab 1.1 step 6, the promotion.** Split brain is worth showing once on the
  projector with two terminals side by side. Let them do steps 1-5 themselves.
- **Lab 2.2 layout D.** If the room is behind, run D yourself and share the numbers.
  It is the slowest cell of the day and the lesson survives being watched.

## Failure modes that eat time

| Symptom | Cause | Fix |
|---|---|---|
| `no configuration file provided` | not in `DataEng_Course/Day1/labs` | `cd` there, or pass `-f DataEng_Course/Day1/labs/docker-compose.yml` |
| `pg_replica` restarts in a loop | primary was not ready when basebackup ran | `docker compose rm -sfv pg_replica && docker volume rm adetc_pgreplica && docker compose up -d pg_replica` |
| PySpark cell hangs on first S3 write | it is downloading the hadoop-aws jars | wait it out once; it caches in the image layer after the first run |
| Trino `SCHEMA_NOT_FOUND` | the file metastore volume was reset | re-run the `CREATE SCHEMA` from lab 2.1 step 4 |
| Neo4j `LOAD CSV` cannot find the file | `fetch.sh` was run from the wrong directory | files must be in `Day1/labs/import/`, not `Day1/labs/data/`; re-run `./data/fetch.sh` from `Day1/labs` |
| `Couldn't connect to docker daemon` on Linux | user not in the docker group | `sudo usermod -aG docker $USER` and log out |
| Marimo build fails on apt | corporate proxy | run marimo on the host: `cd DataEng_Course/Day1/labs/marimo && uv sync && uv run marimo edit ../work` |

## If you are behind

Cut in this order:

1. Lab 1.2 step 6 (write cost) — quote the numbers instead
2. Lab 2.1 step 3 (2,000 small objects) — demo it, it is 4 minutes of waiting
3. Lab 3.2 part 1a (the unindexed load) — this hurts to cut, it is the best lesson in
   the lab, so cut part 2's PageRank write-back first
4. Lab 3.2 entirely, and open Day 2 with it

Never cut lab 2.2. It is the one that changes how people work.

## Grouping

Pairs, on one machine. The second person reads the lab sheet aloud and records the
numbers while the first drives. Swap at each lab. Solo learners fall behind on the
recording, which is the part that makes the discussion work.
