from dagster import AssetExecutionContext, asset
import pandas as pd
import pyarrow.parquet as pq
import requests


@asset(
    name="ApiToPostgres",
    io_manager_key="pandas_postgres_io_manager",
    description="Download dataset from source and save using IOManager",
)
def api_to_postgres(context) -> pd.DataFrame:
    path = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

    df = pd.read_parquet(path)

    return df


@asset(
    name="ApiToPostgresBatch",
    description="Download dataset from source and save using batch processing",
    required_resource_keys={"postgres_resource"},
)
def api_to_postgres_batch(context: AssetExecutionContext) -> None:
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
    response = requests.get(url)
    with open("yellow_tripdata_2021-01.parquet", "wb") as file:
        file.write(response.content)
    context.log.info("Parquet file downloaded.")

    file = pq.ParquetFile("yellow_tripdata_2021-01.parquet")
    df = next(file.iter_batches(batch_size=10)).to_pandas()
    df_iter = file.iter_batches(batch_size=100000)

    engine = context.resources.postgres_resource
    df.head(0).to_sql("ApiToPostgresBatch", engine, if_exists="replace")

    for batch in df_iter:
        df = batch.to_pandas()
        df.to_sql("ApiToPostgresBatch", engine, if_exists="append", index=False)

    context.log.info("Last batch: {batch}")
