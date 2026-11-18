from datetime import date

from conftest import make_payload

from accra_weather_pipeline import config, extract, load
from accra_weather_pipeline.flow import weather_pipeline


def test_the_flow_runs_end_to_end_without_the_internet(tmp_path, monkeypatch):
    database = str(tmp_path / "test.sqlite3")
    monkeypatch.setattr(config, "DATABASE", database)
    monkeypatch.setattr(config, "BRONZE_DIR", str(tmp_path / "bronze"))
    monkeypatch.setattr(extract, "fetch_weather", lambda *args: make_payload())

    weather_pipeline(location="accra", start=date(2026, 1, 1), end=date(2026, 1, 3))

    assert load.count_rows(database)["weather_daily"] == 3
    assert len(list((tmp_path / "bronze").rglob("*.json"))) == 1


def test_a_temporary_failure_is_retried(tmp_path, monkeypatch):
    from accra_weather_pipeline import flow as flow_module

    calls = []

    def flaky_fetch(*args):
        calls.append(1)
        if len(calls) == 1:
            raise extract.TemporarySourceError("Simulated timeout")
        return make_payload()

    monkeypatch.setattr(config, "DATABASE", str(tmp_path / "test.sqlite3"))
    monkeypatch.setattr(extract, "fetch_weather", flaky_fetch)
    # Don't wait between retries in a test.
    monkeypatch.setattr(flow_module, "fetch", flow_module.fetch.with_options(retry_delay_seconds=0))

    weather_pipeline(start=date(2026, 1, 1), end=date(2026, 1, 3), write_bronze=False)

    assert len(calls) == 2


def test_dry_run_does_not_write(tmp_path, monkeypatch):
    database = str(tmp_path / "test.sqlite3")
    monkeypatch.setattr(config, "DATABASE", database)
    monkeypatch.setattr(extract, "fetch_weather", lambda *args: make_payload())

    weather_pipeline(
        location="accra",
        start=date(2026, 1, 1),
        end=date(2026, 1, 3),
        write_bronze=False,
        dry_run=True,
    )

    assert not (tmp_path / "test.sqlite3").exists()
