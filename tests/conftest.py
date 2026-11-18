"""Shared test setup."""

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def prefect_for_tests():
    """Run every flow in the tests against a throwaway Prefect database."""
    with prefect_test_harness():
        yield


def make_payload(days: int = 3, **changes) -> dict:
    """Build a response shaped like Open-Meteo's, with optional changed fields."""
    daily = {
        "time": [f"2026-01-{day:02d}" for day in range(1, days + 1)],
        "temperature_2m_max": [31.0 + day for day in range(days)],
        "temperature_2m_min": [24.0 + day for day in range(days)],
        "precipitation_sum": [0.0, 2.5, 0.4][:days] + [0.0] * max(0, days - 3),
    }
    daily.update(changes)
    return {"daily": daily}
