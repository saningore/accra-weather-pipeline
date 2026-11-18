-- Supabase / PostgreSQL tables for the Accra weather pipeline.
-- Safe to rerun. Enable RLS so the public Data API returns nothing by default.

CREATE TABLE IF NOT EXISTS weather_daily (
    location TEXT NOT NULL,
    observed_on DATE NOT NULL,
    temp_max_c DOUBLE PRECISION NOT NULL,
    temp_min_c DOUBLE PRECISION NOT NULL,
    precipitation_mm DOUBLE PRECISION NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (location, observed_on)
);

CREATE TABLE IF NOT EXISTS weather_features (
    location TEXT NOT NULL,
    observed_on DATE NOT NULL,
    temp_range_c DOUBLE PRECISION NOT NULL,
    temp_max_7d_avg_c DOUBLE PRECISION,
    is_rainy_day BOOLEAN NOT NULL,
    PRIMARY KEY (location, observed_on)
);

ALTER TABLE weather_daily ENABLE ROW LEVEL SECURITY;
ALTER TABLE weather_features ENABLE ROW LEVEL SECURITY;
