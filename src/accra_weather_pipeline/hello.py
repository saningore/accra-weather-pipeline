"""A hello-world pipeline: three tiny tasks wired together by a flow."""

from prefect import flow, task
from prefect.runtime import task_run


@task(retries=2, retry_delay_seconds=3)
def extract() -> list[int]:
    """Pretend to read data from a source that fails the first time we ask."""
    if task_run.run_count == 1:
        raise ConnectionError("The source didn't answer. Prefect will retry.")
    return [1, 2, 3]


@task
def transform(numbers: list[int]) -> list[int]:
    """Change the data: here, multiply every number by 10."""
    return [number * 10 for number in numbers]


@task
def load(numbers: list[int]) -> None:
    """Pretend to save the data somewhere."""
    print(f"Loaded {len(numbers)} numbers: {numbers}")


@flow(log_prints=True)
def hello_pipeline(name: str = "world") -> list[int]:
    """Run extract, transform and load in order."""
    print(f"Hello, {name}! Starting the pipeline.")
    numbers = extract()
    bigger = transform(numbers)
    load(bigger)
    return bigger


if __name__ == "__main__":
    hello_pipeline()
