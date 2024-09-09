from dagster import asset
import pandas as pd


@asset(name="Hello")
def hello():
    return 1


@asset(name="ByeBye")
def byebye():
    return 2


@asset(name="ApiToPostgres", io_manager_key="pandas_postgres_io_manager")
def api_to_postgres() -> pd.DataFrame:
    path = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

    df = pd.read_parquet(path)

    return df
