"""Transform: compute Gold features from the full Silver history."""

import polars as pl


def build_features(silver: pl.DataFrame) -> pl.DataFrame:
    """Add the values an analyst or a model will use.

    Pass the whole Silver history for a location, not one batch, so that a 7-day
    average at the start of a new batch can still see the days before it.
    """
    return (
        silver.sort("location", "observed_on")
        .with_columns(
            (pl.col("temp_max_c") - pl.col("temp_min_c")).round(1).alias("temp_range_c"),
            pl.col("temp_max_c")
            .rolling_mean(7)
            .over("location")
            .round(2)
            .alias("temp_max_7d_avg_c"),
            (pl.col("precipitation_mm") >= 1.0).alias("is_rainy_day"),
        )
        .select("location", "observed_on", "temp_range_c", "temp_max_7d_avg_c", "is_rainy_day")
    )
