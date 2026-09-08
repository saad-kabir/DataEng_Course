"""Lab 3.1 - an hourly pipeline you can re-run.

Loads one hour of trips from Postgres into two bronze tables, one by APPEND and one
by MERGE, so the difference between them is visible on the second run of the same
interval.

The source is Day 1's `trips` table, whose pickup timestamps are January 2024. The
DAG's start_date sits inside that window so every run has real data to move.

Slide 13 is the point of the whole file: the interval is an INPUT, taken from
`data_interval_start`/`data_interval_end`, never from `now()`. That is what makes a
re-run and a backfill produce the same rows as the original run.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.timetables.interval import CronDataIntervalTimetable

CONN_ID = "rides_pg"

# READ THIS BEFORE CHANGING THE SCHEDULE.
#
# `schedule="@hourly"` does NOT give you a data interval on Airflow 3. Airflow 3 made
# CronTriggerTimetable the default for cron strings: it fires AT the cron time and
# sets data_interval_start == data_interval_end == logical_date. Every window in this
# DAG would then be zero-width, every task would stage zero rows, and every run would
# still report success.
#
# CronDataIntervalTimetable is the Airflow 2 behaviour the deck describes on slide 13:
# a run covers the hour BEFORE its trigger time. Asking for it explicitly is the whole
# reason this DAG works. Slide 17's "one hour behind its start time" depends on it.
HOURLY_INTERVAL = CronDataIntervalTimetable("0 * * * *", timezone="UTC")


def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONN_ID)


@dag(
    dag_id="trips_hourly",
    schedule=HOURLY_INTERVAL,
    start_date=pendulum.datetime(2024, 1, 15, 0, 0, tz="UTC"),
    # catchup=False on purpose (slide 14): a paused DAG resuming with catchup on
    # would launch every missed interval since the start date at once.
    catchup=False,
    max_active_runs=2,
    default_args={"retries": 2, "retry_delay": pendulum.duration(seconds=30)},
    tags=["day3", "lab-3.1"],
    doc_md=__doc__,
)
def trips_hourly():

    @task
    def extract(data_interval_start=None, data_interval_end=None) -> int:
        """Copy exactly this interval's rows into staging.

        DELETE-then-INSERT for the interval, so the task owns its window and a
        re-run replaces its own rows rather than adding to them.
        """
        hook = _hook()
        # Two separate statements, not one string: psycopg prepares any statement
        # that carries parameters, and a prepared statement cannot hold multiple
        # commands ("cannot insert multiple commands into a prepared statement").
        # PostgresHook.run takes a list and executes them in one transaction.
        hook.run(
            [
                "DELETE FROM bronze.trips_staging WHERE interval_start = %(start)s",
                """INSERT INTO bronze.trips_staging
                       (interval_start, trip_id, pickup_ts, pu_zone_id, fare_amount)
                   SELECT %(start)s, trip_id, pickup_ts, pu_zone_id, fare_amount
                     FROM trips
                    WHERE pickup_ts >= %(start)s AND pickup_ts < %(end)s""",
            ],
            parameters={"start": data_interval_start, "end": data_interval_end},
        )
        rows = hook.get_first(
            "SELECT count(*) FROM bronze.trips_staging WHERE interval_start = %(start)s",
            parameters={"start": data_interval_start},
        )[0]
        print(f"staged {rows} rows for [{data_interval_start}, {data_interval_end})")
        return rows

    @task
    def load_append(data_interval_start=None) -> int:
        """The naive write. Correct exactly once, wrong on every re-run."""
        hook = _hook()
        hook.run(
            """
            INSERT INTO bronze.trips_append (trip_id, pickup_ts, pu_zone_id, fare_amount)
            SELECT trip_id, pickup_ts, pu_zone_id, fare_amount
              FROM bronze.trips_staging WHERE interval_start = %(start)s;
            """,
            parameters={"start": data_interval_start},
        )
        total = hook.get_first("SELECT count(*) FROM bronze.trips_append")[0]
        print(f"trips_append now holds {total} rows")
        return total

    @task
    def load_merge(data_interval_start=None) -> int:
        """The idempotent write (slide 7). Running it twice changes nothing."""
        hook = _hook()
        hook.run(
            """
            MERGE INTO bronze.trips_merge t
            USING (SELECT trip_id, pickup_ts, pu_zone_id, fare_amount
                     FROM bronze.trips_staging WHERE interval_start = %(start)s) s
               ON t.trip_id = s.trip_id
             WHEN MATCHED THEN UPDATE SET
                   pickup_ts = s.pickup_ts,
                   pu_zone_id = s.pu_zone_id,
                   fare_amount = s.fare_amount
             WHEN NOT MATCHED THEN INSERT (trip_id, pickup_ts, pu_zone_id, fare_amount)
                   VALUES (s.trip_id, s.pickup_ts, s.pu_zone_id, s.fare_amount);
            """,
            parameters={"start": data_interval_start},
        )
        total = hook.get_first("SELECT count(*) FROM bronze.trips_merge")[0]
        print(f"trips_merge now holds {total} rows")
        return total

    staged = extract()
    staged >> load_append()
    staged >> load_merge()


trips_hourly()
