"""Keep prefect.yaml pip_packages aligned with uv.lock."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_pip_packages_match_lockfile_direct_deps():
    prefect_yaml = yaml.safe_load((ROOT / "prefect.yaml").read_text(encoding="utf-8"))
    deployment = next(d for d in prefect_yaml["deployments"] if d["name"] == "daily-weather")
    pins = deployment["work_pool"]["job_variables"]["pip_packages"]

    # Exact pins used by Managed workers; must match `uv tree --depth 1 --no-dev`.
    expected = {
        "prefect==3.9.3",
        "polars==2.1.0",
        "httpx==0.28.1",
        "python-dotenv==1.2.4",
        "psycopg[binary]==3.3.9",
    }
    assert set(pins) == expected
