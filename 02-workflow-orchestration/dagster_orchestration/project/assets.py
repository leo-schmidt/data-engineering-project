from dagster import AssetExecutionContext, AssetIn, asset
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

    context.log.info(f"Last batch: {batch}")


@asset(
    name="ApiToGCS",
    io_manager_key="gcs_io_manager",
    description="Download dataset from source and upload to GCS bucket",
)
def api_to_gcs() -> pd.DataFrame:
    path = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

    df = pd.read_parquet(path)

    return df


@asset(
    name="GcsToBigQuery",
    io_manager_key="bigquery_io_manager",
    description="Download dataset from GCS and upload to BigQuery",
    ins={"parquet_file": AssetIn("ApiToGCS", input_manager_key="gcs_io_manager")},
)
def gcs_to_bigquery(context: AssetExecutionContext, parquet_file) -> pd.DataFrame:
    # load parquet using gcs_io_manager
    # and return it to be stored using bigquery_io_manager
    return parquet_file
