# Accra weather pipeline

Batch pipeline that loads **daily Open-Meteo weather** for Accra (and optionally Kumasi) into Silver and Gold tables. Prefect orchestrates extract → validate → load; Polars enforces the data contract; SQLite (local) or PostgreSQL/Supabase (production) stores upserts.

## Quickstart (offline)

```sh
cp .env.example .env   # PowerShell: Copy-Item .env.example .env
uv sync --locked
uv run ruff check .
uv run pytest
```

Replay the packaged fixture through validation:

```sh
uv run python -c "import json; from pathlib import Path; from accra_weather_pipeline.validate import validate_weather; p=Path('src/accra_weather_pipeline/data/sample_weather.json'); r=json.loads(p.read_text()); print(validate_weather(r['payload'], r['_location']))"
```

## Run locally (needs network for Open-Meteo)

```sh
# Optional: local Prefect UI in another terminal
# uv run prefect profile use local && uv run prefect server start

uv run accra-weather-pipeline
uv run accra-weather-pipeline --location kumasi
uv run accra-weather-pipeline --dry-run
uv run accra-weather-pipeline --start 2026-09-01 --end 2026-09-30
```

| Command | Meaning |
| --- | --- |
| no dates | Catch-up from watermark (first run loads ~30 days) |
| `--start` / `--end` | Backfill an explicit range |
| `--dry-run` | Validate only; **no database write** |
| `--serve` | Local schedule at 06:30 UTC (terminal must stay open) |

Hello-world / plan sketch (Labs 3–4):

```sh
uv run python src/accra_weather_pipeline/hello.py
uv run python src/accra_weather_pipeline/plan_sketch.py
```

## Architecture

| Layer | What | Where |
| --- | --- | --- |
| Source | Open-Meteo historical daily weather | `extract.fetch_weather` |
| Bronze | Raw JSON + `_source`, `_location`, `_ingested_at` | `data/bronze/…` (local) |
| Silver | Typed, validated rows | `weather_daily` |
| Gold | Range, 7-day max avg, rainy-day flag | `weather_features` |

Record key: `(location, observed_on)`. Loads use `ON CONFLICT … DO UPDATE` so reruns are idempotent. Silver and Gold commit in one transaction.

## Deploy to Prefect Cloud

1. Create a **Prefect Managed** work pool named `my-managed-pool`.
2. Create a **Secret** block `accra-weather-database` with your Postgres URI (`sslmode=require`). For hello-only demos you can skip this and run the `hello` deployment.
3. Apply `sql/schema.sql` in Supabase (or your Postgres).
4. Push to GitHub, then either:
   - **CI:** set GitHub variable `PREFECT_API_URL`, secret `PREFECT_API_KEY`, and variable `ENABLE_PREFECT_DEPLOY=true`, then merge to `main`.
   - **Laptop (hello first):** push, then from `src/`:

```sh
uv run prefect profile use cloud
# Lab 4 style (branch) for first hello deploy — or set PREFECT_CODE_REVISION for production yaml
PREFECT_CODE_REVISION=$(git rev-parse HEAD)  # Git Bash / macOS / Linux
# PowerShell: $env:PREFECT_CODE_REVISION = (git rev-parse HEAD)
uv run prefect deploy --prefect-file ../prefect.yaml --name hello --no-prompt
uv run prefect deployment run 'hello-pipeline/hello' --watch
```

Production schedule (`daily-weather`) is defined in `prefect.yaml` with `active: false` until you are ready. Pause schedules on the free tier when not demonstrating.

## Runbook

**What it does:** loads yesterday’s weather for Accra from Open-Meteo into Silver (`weather_daily`) and Gold (`weather_features`).  
**Schedule:** Prefect deployment `weather-pipeline/daily-weather` (cron `30 6 * * *` UTC when activated).  
**Releases:** merge a PR into `main`; GitHub Actions runs checks and, when enabled, registers the commit in Prefect Cloud.

| Task | How |
| --- | --- |
| See runs / logs | Prefect Cloud → Runs, filter `weather-pipeline` |
| Run now | `uv run prefect deployment run 'weather-pipeline/daily-weather'` |
| Backfill | `uv run prefect deployment run 'weather-pipeline/daily-weather' -p start=YYYY-MM-DD -p end=YYYY-MM-DD` |
| Dry run locally | `uv run accra-weather-pipeline --dry-run --start YYYY-MM-DD --end YYYY-MM-DD` |
| Pause schedule | Toggle off on the deployment page |
| Rotate DB password | Update Supabase password and the `accra-weather-database` Secret block |
| Run locally | `uv sync --locked` then `uv run accra-weather-pipeline` (SQLite) |

**Common failures**

| Error | Meaning | Action |
| --- | --- | --- |
| `TemporarySourceError` after retries | API down / rate limited | Wait; next catch-up run recovers |
| `missing fields: …` | Schema drift (required field) | Inspect Bronze / API docs; update contract in a PR |
| `password authentication failed` | Bad DB credential | Update the Secret block |

**Known limitations**

- The 7-day average counts **rows**, not calendar days, so gaps change the window.
- Bronze is not durable in Prefect Managed runs (`write_bronze: false`); keep Bronze locally or add object storage later.
- Open-Meteo historical data can revise recent days; overlap of 3 days absorbs that.

## Project layout

```text
src/accra_weather_pipeline/   # package
tests/                        # pytest (offline)
docs/plan.md                  # Lab 1 plan
sql/schema.sql                # Supabase DDL
prefect.yaml                  # Cloud deployments
.github/workflows/ci.yml      # checks + optional deploy
```

## Security notes

- `.env` is gitignored; only `.env.example` is committed.
- Production DB URI lives in a Prefect **Secret** block, not in GitHub.
- GitHub holds `PREFECT_API_KEY` as a **secret** and `PREFECT_API_URL` / `ENABLE_PREFECT_DEPLOY` as **variables**.
- CI pins Actions to full commit SHAs and runs `uv audit`.
