from dagster import (
    AssetExecutionContext,
    AssetIn,
    MonthlyPartitionsDefinition,
    asset,
)
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
    name="YellowTaxiApiToGCS",
    io_manager_key="gcs_io_manager",
    description="Download dataset from source and upload to GCS bucket",
    partitions_def=MonthlyPartitionsDefinition("2021-01-01", "2023-01-01"),
)
def yellow_taxi_api_to_gcs(context: AssetExecutionContext) -> pd.DataFrame:
    month_key = context.partition_key[:-3]
    path = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_key}.parquet"

    df = pd.read_parquet(path)

    return df


@asset(
    name="GreenTaxiApiToGCS",
    io_manager_key="gcs_io_manager",
    description="Download dataset from source and upload to GCS bucket",
    partitions_def=MonthlyPartitionsDefinition("2021-01-01", "2023-01-01"),
)
def green_taxi_api_to_gcs(context: AssetExecutionContext) -> pd.DataFrame:
    month_key = context.partition_key[:-3]
    path = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{month_key}.parquet"

    df = pd.read_parquet(path)

    return df


@asset(
    name="YellowTaxiGcsToBigQuery",
    io_manager_key="bigquery_io_manager",
    description="Download dataset from GCS and upload to BigQuery",
    ins={
        "parquet_file": AssetIn(
            "YellowTaxiApiToGCS", input_manager_key="gcs_io_manager"
        )
    },
    partitions_def=MonthlyPartitionsDefinition("2021-01-01", "2023-01-01"),
    metadata={"partition_expr": "LPEP_PICKUP_DATETIME"},
)
def yellow_taxi_gcs_to_bigquery(
    context,
    parquet_file: pd.DataFrame,
) -> pd.DataFrame:
    # load parquet using gcs_io_manager
    # and return it to be stored using bigquery_io_manager
    return parquet_file


@asset(
    name="GreenTaxiGcsToBigQuery",
    io_manager_key="bigquery_io_manager",
    description="Download dataset from GCS and upload to BigQuery",
    ins={
        "parquet_file": AssetIn(
            "GreenTaxiApiToGCS",
            input_manager_key="gcs_io_manager",
        )
    },
    partitions_def=MonthlyPartitionsDefinition("2021-01-01", "2023-01-01"),
    metadata={"partition_expr": "LPEP_PICKUP_DATETIME"},
)
def green_taxi_gcs_to_bigquery(
    context,
    parquet_file: pd.DataFrame,
) -> pd.DataFrame:
    return parquet_file
