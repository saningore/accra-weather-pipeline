from conftest import make_payload

from accra_weather_pipeline import load
from accra_weather_pipeline.validate import validate_weather


def test_loading_the_same_batch_twice_changes_nothing(tmp_path):
    database = str(tmp_path / "test.sqlite3")
    silver = validate_weather(make_payload(), "accra")

    load.save(database, silver)
    load.save(database, silver)

    assert load.count_rows(database) == {"weather_daily": 3, "weather_features": 3}
    assert str(load.latest_date(database, "accra")) == "2026-01-03"


def test_gold_error_rolls_back_silver(tmp_path, monkeypatch):
    database = str(tmp_path / "test.sqlite3")
    silver = validate_weather(make_payload(), "accra")
    load.save(database, silver)
    before = load.count_rows(database)

    def boom(_history):
        raise RuntimeError("boom")

    monkeypatch.setattr(load, "build_features", boom)
    try:
        load.save(database, silver)
    except RuntimeError:
        pass

    assert load.count_rows(database) == before
