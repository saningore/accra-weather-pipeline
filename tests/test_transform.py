import polars as pl
from conftest import make_payload

from accra_weather_pipeline.transform import build_features
from accra_weather_pipeline.validate import validate_weather


def test_features_are_computed_per_row():
    gold = build_features(validate_weather(make_payload(), "accra"))
    assert gold["temp_range_c"].to_list() == [7.0, 7.0, 7.0]
    assert gold["is_rainy_day"].to_list() == [False, True, False]


def test_the_7_day_average_waits_for_7_days():
    gold = build_features(validate_weather(make_payload(days=8), "accra"))
    averages = gold["temp_max_7d_avg_c"].to_list()
    assert averages[:6] == [None] * 6
    assert averages[6] == 34.0  # mean of 31, 32, ... 37
    assert gold.filter(pl.col("temp_max_7d_avg_c").is_not_null()).height == 2
