"""Validate: turn the raw response into a typed Silver table, or reject the whole batch."""

import polars as pl

FIELDS = {
    "time": "observed_on",
    "temperature_2m_max": "temp_max_c",
    "temperature_2m_min": "temp_min_c",
    "precipitation_sum": "precipitation_mm",
}
KEY = ["location", "observed_on"]


def unexpected_fields(payload: dict) -> list[str]:
    """List fields the source sent that the contract doesn't use: an early sign of drift."""
    daily = payload.get("daily")
    return sorted(daily.keys() - FIELDS.keys()) if isinstance(daily, dict) else []


def validate_weather(payload: dict, location: str) -> pl.DataFrame:
    """Check the data contract and return clean Silver rows.

    Raises:
        ValueError: A field is missing or any row breaks a rule. One bad row rejects the batch.
    """
    daily = payload.get("daily")
    if not isinstance(daily, dict):
        daily = {}  # Treat a missing or malformed section as "every field is missing".
    missing = sorted(FIELDS.keys() - daily.keys())
    if missing:
        raise ValueError(f"{location}: missing fields: {', '.join(missing)}.")
    if len({len(daily[name]) for name in FIELDS}) != 1:
        raise ValueError(f"{location}: the fields have different numbers of values.")

    raw = pl.DataFrame({name: daily[name] for name in FIELDS}, strict=False)
    silver = raw.select(
        pl.lit(location).alias("location"),
        pl.col("time").cast(pl.String).str.to_date("%Y-%m-%d", strict=False).alias("observed_on"),
        *[
            pl.col(source).cast(pl.Float64, strict=False).alias(target)
            for source, target in FIELDS.items()
            if source != "time"
        ],
    )

    temperatures = pl.col("temp_max_c", "temp_min_c")
    rules = {
        "a value is missing or has the wrong type": pl.any_horizontal(pl.all().is_null()),
        "temperature is outside -90 to 60 °C": pl.any_horizontal(~temperatures.is_between(-90, 60)),
        "the minimum temperature is above the maximum": pl.col("temp_min_c") > pl.col("temp_max_c"),
        "precipitation is negative": pl.col("precipitation_mm") < 0,
        "the same location and date appear twice": pl.struct(KEY).is_duplicated(),
    }
    for problem, broken in rules.items():
        bad_rows = silver.filter(broken)
        if bad_rows.height:
            examples = bad_rows["observed_on"].head(3).to_list()
            raise ValueError(f"{location}: {problem} in {bad_rows.height} row(s), e.g. {examples}.")
    return silver.sort(KEY)
