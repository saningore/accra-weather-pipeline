# Pipeline plan

## Source
- What: daily weather (max/min temperature, rainfall) for Accra and Kumasi
- Where: Open-Meteo historical API, no key needed
- Format: JSON, one list per field (`daily.time`, `daily.temperature_2m_max`, …)
- Terms: free for non-commercial use (https://open-meteo.com/en/terms)

## Record key
- One record = one location on one day
- Key: location + observed_on

## Data contract (what "valid" means)
- Required fields: time, temperature_2m_max, temperature_2m_min, precipitation_sum
- Types: date, number, number, number
- Rules:
  1. temperatures between -90 and 60 °C
  2. min temperature <= max temperature
  3. rainfall >= 0
  4. no duplicate location + date

## Layers
- Bronze: the JSON response exactly as received, plus `_source`, `_location`, `_ingested_at`
- Silver: one typed row per location and day (`weather_daily`)
- Gold: daily temperature range, 7-day average max temperature, rainy-day flag (`weather_features`)

## Cadence
- The source adds one day per day and revises recent days for a while
- Run once a day, re-reading the last 3 days each time (overlap)

## What could go wrong
- API down or slow → retry temporary errors only
- A field renamed or removed → reject the batch, keep the Bronze copy
- A new unexpected field → log a schema-drift warning; Silver ignores it until the contract evolves
- A missed day → the next run catches up from the watermark
