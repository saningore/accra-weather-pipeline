"""Orchestrate: wire the steps into a Prefect flow with retries, catch-up and backfill."""

import argparse
from datetime import UTC, date, datetime, timedelta

import polars as pl
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE

from accra_weather_pipeline import config, extract, load
from accra_weather_pipeline.validate import unexpected_fields, validate_weather


def is_temporary(_task, _task_run, state) -> bool:
    """Tell Prefect to retry only errors that might go away on their own."""
    return isinstance(state.result(raise_on_failure=False), extract.TemporarySourceError)


@task(
    retries=3,
    retry_delay_seconds=[5, 30, 60],
    retry_condition_fn=is_temporary,
    cache_policy=NO_CACHE,
)
def fetch(location: str, start: date, end: date) -> dict:
    latitude, longitude = config.LOCATIONS[location]
    return extract.fetch_weather(latitude, longitude, start, end)


@task(cache_policy=NO_CACHE)
def keep_bronze(payload: dict, location: str) -> None:
    path = extract.save_bronze(payload, location, config.BRONZE_DIR)
    get_run_logger().info("Bronze copy saved to %s", path)


@task(cache_policy=NO_CACHE)
def check(payload: dict, location: str) -> pl.DataFrame:
    extra = unexpected_fields(payload)
    if extra:
        get_run_logger().warning(
            "Schema drift: the source sent new fields %s. Silver ignores them until you "
            "add them to the contract.",
            extra,
        )
    return validate_weather(payload, location)


@task(cache_policy=NO_CACHE)
def store(database: str, silver: pl.DataFrame) -> int:
    return load.save(database, silver)


@flow(name="weather-pipeline", log_prints=True)
def weather_pipeline(
    location: str = "accra",
    start: date | None = None,
    end: date | None = None,
    write_bronze: bool = True,
    dry_run: bool = False,
    database_secret: str | None = None,
) -> None:
    """Load daily weather for one location.

    With no dates, the flow catches up: it starts a few days before the newest stored
    date (the watermark) and stops at yesterday. Give start and end to backfill a range.
    Set database_secret to the name of a Prefect Secret block that holds a PostgreSQL URL.
    dry_run validates and builds features but does not write to the database.
    """
    database = Secret.load(database_secret).get() if database_secret else config.DATABASE
    end = end or datetime.now(UTC).date() - timedelta(days=1)  # Today isn't finished yet.
    if start is None:
        latest = None if dry_run else load.latest_date(database, location)
        if latest is None:
            start = end - timedelta(days=config.FIRST_LOAD_DAYS)
        else:
            start = latest - timedelta(days=config.OVERLAP_DAYS)
    if start > end:
        raise ValueError(f"start {start} is after end {end}.")
    print(f"Loading {location} from {start} to {end}.")

    payload = fetch(location, start, end)
    if write_bronze:
        keep_bronze(payload, location)
    silver = check(payload, location)
    if dry_run:
        print(f"Dry run: validated {silver.height} Silver rows; no database write.")
        return
    gold_rows = store(database, silver)
    print(f"Saved {silver.height} Silver rows and refreshed {gold_rows} Gold rows.")


def main() -> None:
    """Command line: run once, backfill a range, dry-run, or serve on a schedule."""
    parser = argparse.ArgumentParser(description="Load daily weather into the database.")
    parser.add_argument("--location", default="accra", choices=sorted(config.LOCATIONS))
    parser.add_argument("--start", type=date.fromisoformat, help="First day, YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, help="Last day, YYYY-MM-DD")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate without writing to the database",
    )
    parser.add_argument("--serve", action="store_true", help="Run every day at 06:30 UTC")
    args = parser.parse_args()
    if args.serve:
        weather_pipeline.serve(
            name="daily", cron="30 6 * * *", parameters={"location": args.location}
        )
    else:
        weather_pipeline(
            location=args.location,
            start=args.start,
            end=args.end,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            print(load.count_rows(config.DATABASE))
