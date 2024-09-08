from dagster import asset


@asset(name="Hello")
def hello():
    return 1


@asset(name="ByeBye")
def byebye():
    return 2
