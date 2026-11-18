"""Extract: ask the source for data and keep an untouched Bronze copy."""

import json
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import httpx

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_FIELDS = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]


class TemporarySourceError(RuntimeError):
    """The source failed in a way that might work if we try again."""


def fetch_weather(latitude: float, longitude: float, start: date, end: date) -> dict:
    """Download daily weather for one place, from start to end (both included).

    Raises:
        TemporarySourceError: Network trouble, rate limiting or a server error.
        httpx.HTTPStatusError: Any other bad answer, such as a wrong parameter.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(DAILY_FIELDS),
        "timezone": "UTC",
    }
    try:
        response = httpx.get(ARCHIVE_URL, params=params, timeout=30)
    except httpx.TransportError as error:
        raise TemporarySourceError(f"Could not reach Open-Meteo: {error}") from error
    if response.status_code == 429 or response.status_code >= 500:
        raise TemporarySourceError(f"Open-Meteo answered {response.status_code}. Try later.")
    response.raise_for_status()
    return response.json()


def save_bronze(payload: dict, location: str, bronze_dir: str) -> Path:
    """Save the response exactly as received, plus metadata about where it came from."""
    now = datetime.now(UTC)
    folder = Path(bronze_dir) / f"ingest_date={now.date()}" / f"location={location}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{uuid4().hex}.json"
    record = {
        "_source": ARCHIVE_URL,
        "_location": location,
        "_ingested_at": now.isoformat(),
        "payload": payload,
    }
    path.write_text(json.dumps(record, indent=2))
    return path
