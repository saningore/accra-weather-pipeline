"""Load: save Silver and Gold so that loading the same batch twice changes nothing."""

import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import date
from pathlib import Path

import polars as pl
import psycopg

from accra_weather_pipeline.transform import build_features

sqlite3.register_adapter(date, date.isoformat)  # Store dates as YYYY-MM-DD text in SQLite.

TABLES = """
CREATE TABLE IF NOT EXISTS weather_daily (
    location TEXT NOT NULL,
    observed_on DATE NOT NULL,
    temp_max_c DOUBLE PRECISION NOT NULL,
    temp_min_c DOUBLE PRECISION NOT NULL,
    precipitation_mm DOUBLE PRECISION NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (location, observed_on)
);
CREATE TABLE IF NOT EXISTS weather_features (
    location TEXT NOT NULL,
    observed_on DATE NOT NULL,
    temp_range_c DOUBLE PRECISION NOT NULL,
    temp_max_7d_avg_c DOUBLE PRECISION,
    is_rainy_day BOOLEAN NOT NULL,
    PRIMARY KEY (location, observed_on)
);
"""
# {p} becomes the driver's placeholder: ? for SQLite and %s for PostgreSQL.
UPSERT_SILVER = """
INSERT INTO weather_daily (location, observed_on, temp_max_c, temp_min_c, precipitation_mm)
VALUES ({p}, {p}, {p}, {p}, {p})
ON CONFLICT (location, observed_on) DO UPDATE SET
    temp_max_c = excluded.temp_max_c,
    temp_min_c = excluded.temp_min_c,
    precipitation_mm = excluded.precipitation_mm,
    loaded_at = CURRENT_TIMESTAMP
"""
UPSERT_GOLD = """
INSERT INTO weather_features
    (location, observed_on, temp_range_c, temp_max_7d_avg_c, is_rainy_day)
VALUES ({p}, {p}, {p}, {p}, {p})
ON CONFLICT (location, observed_on) DO UPDATE SET
    temp_range_c = excluded.temp_range_c,
    temp_max_7d_avg_c = excluded.temp_max_7d_avg_c,
    is_rainy_day = excluded.is_rainy_day
"""
SILVER_COLUMNS = ["location", "observed_on", "temp_max_c", "temp_min_c", "precipitation_mm"]


@contextmanager
def open_database(database: str) -> Iterator[tuple[sqlite3.Cursor | psycopg.Cursor, str]]:
    """Open the database, create the tables, and commit only if everything succeeds."""
    if database.startswith(("postgresql://", "postgres://")):
        with psycopg.connect(database, connect_timeout=10) as connection:
            connection.execute(TABLES)
            with connection.cursor() as cursor:
                yield cursor, "%s"
        return
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database)) as connection:
        connection.executescript(TABLES)
        with connection:  # Commits on success, rolls back on any error.
            yield connection.cursor(), "?"


def save(database: str, silver: pl.DataFrame) -> int:
    """Upsert Silver, rebuild Gold from all stored Silver, and commit both together.

    Returns:
        The number of Gold rows written.
    """
    locations = silver["location"].unique().to_list()
    with open_database(database) as (cursor, p):
        cursor.executemany(UPSERT_SILVER.format(p=p), silver.select(SILVER_COLUMNS).rows())
        query = f"SELECT {', '.join(SILVER_COLUMNS)} FROM weather_daily WHERE location = {p}"
        history = []
        for location in locations:
            cursor.execute(query, (location,))
            history += cursor.fetchall()
        stored = pl.DataFrame(history, schema=SILVER_COLUMNS, orient="row").with_columns(
            pl.col("observed_on").cast(pl.String).str.to_date()
        )
        gold = build_features(stored)
        cursor.executemany(UPSERT_GOLD.format(p=p), gold.rows())
    return gold.height


def latest_date(database: str, location: str) -> date | None:
    """Return the newest stored date for a location: the pipeline's watermark."""
    with open_database(database) as (cursor, p):
        cursor.execute(
            f"SELECT MAX(observed_on) FROM weather_daily WHERE location = {p}", (location,)
        )
        (latest,) = cursor.fetchone()
    return date.fromisoformat(str(latest)) if latest else None


def count_rows(database: str) -> dict[str, int]:
    """Count the rows in each table: a quick check that reruns add nothing."""
    with open_database(database) as (cursor, _):
        counts = {}
        for table in ("weather_daily", "weather_features"):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
    return counts
