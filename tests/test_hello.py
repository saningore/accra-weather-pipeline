from accra_weather_pipeline.hello import hello_pipeline


def test_hello_pipeline_multiplies_by_ten():
    assert hello_pipeline(name="test") == [10, 20, 30]
