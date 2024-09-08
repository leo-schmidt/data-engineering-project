from dagster import asset


@asset(name="Hello")
def hello():
    return 1
