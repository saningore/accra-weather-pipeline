"""Sketch of the real weather pipeline: named steps matching docs/plan.md."""

from prefect import flow, task
from prefect.runtime import task_run


@task(retries=2, retry_delay_seconds=3)
def fetch_weather() -> list[str]:
    """Stand-in for Open-Meteo extract (fails once to show retries)."""
    if task_run.run_count == 1:
        raise ConnectionError("Open-Meteo timed out. Prefect will retry.")
    return ["accra-2026-01-01", "accra-2026-01-02", "accra-2026-01-03"]


@task
def check_weather(days: list[str]) -> list[str]:
    """Stand-in for Silver validation against the data contract."""
    print(f"Checking contract for {len(days)} day(s).")
    return days


@task
def save_weather(days: list[str]) -> None:
    """Stand-in for the idempotent Silver + Gold load."""
    print(f"Would upsert {len(days)} Silver rows and refresh Gold features.")


@flow(log_prints=True)
def weather_plan_sketch(location: str = "accra") -> list[str]:
    """Draw the real pipeline shape before the production flow exists."""
    print(f"Plan sketch for {location}: fetch → check → save.")
    days = fetch_weather()
    trusted = check_weather(days)
    save_weather(trusted)
    return trusted


if __name__ == "__main__":
    weather_plan_sketch()
