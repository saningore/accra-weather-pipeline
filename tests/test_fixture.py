import json
from pathlib import Path

from accra_weather_pipeline.validate import validate_weather

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "accra_weather_pipeline"
    / "data"
    / "sample_weather.json"
)


def test_packaged_fixture_validates():
    record = json.loads(FIXTURE.read_text(encoding="utf-8"))
    silver = validate_weather(record["payload"], record["_location"])
    assert silver.height == 3
    assert silver["location"].unique().to_list() == ["accra"]
