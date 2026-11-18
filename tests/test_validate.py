import pytest
from conftest import make_payload

from accra_weather_pipeline.validate import unexpected_fields, validate_weather


def test_a_good_batch_becomes_typed_silver_rows():
    silver = validate_weather(make_payload(), "accra")
    assert silver.height == 3
    assert silver.columns == [
        "location",
        "observed_on",
        "temp_max_c",
        "temp_min_c",
        "precipitation_mm",
    ]
    assert str(silver["observed_on"].dtype) == "Date"


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"temperature_2m_min": [40.0, 24.0, 25.0]}, "minimum temperature is above"),
        ({"precipitation_sum": [0.0, -1.0, 0.0]}, "precipitation is negative"),
        ({"temperature_2m_max": [31.0, None, 33.0]}, "missing or has the wrong type"),
        ({"time": ["2026-01-01", "2026-01-01", "2026-01-03"]}, "appear twice"),
        ({"time": ["2026-01-01", "2026-01-02"]}, "different numbers of values"),
    ],
)
def test_one_bad_row_rejects_the_whole_batch(changes, message):
    with pytest.raises(ValueError, match=message):
        validate_weather(make_payload(**changes), "accra")


def test_a_missing_field_is_named():
    payload = make_payload()
    del payload["daily"]["precipitation_sum"]
    with pytest.raises(ValueError, match="missing fields: precipitation_sum"):
        validate_weather(payload, "accra")


def test_new_source_fields_are_noticed_but_dont_break_the_batch():
    payload = make_payload(rain_sum=[0.0, 2.0, 0.0])
    assert unexpected_fields(payload) == ["rain_sum"]
    assert validate_weather(payload, "accra").height == 3
