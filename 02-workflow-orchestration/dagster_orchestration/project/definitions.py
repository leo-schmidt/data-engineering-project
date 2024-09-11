import os
from dagster import Definitions, load_assets_from_modules
from dagster_gcp import GCSPickleIOManager, GCSResource
from dagster_gcp_pandas import BigQueryPandasIOManager

from . import assets
from .io_manager import pandas_postgres_io_manager
from .resources import postgres_resource


all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "pandas_postgres_io_manager": pandas_postgres_io_manager.configured(
            {
                "username": {"env": "DAGSTER_POSTGRES_USER"},
                "password": {"env": "DAGSTER_POSTGRES_PASSWORD"},
                "hostname": {"env": "DAGSTER_POSTGRES_HOST"},
                "db_name": {"env": "DAGSTER_POSTGRES_DB"},
                "port": {"env": "DAGSTER_POSTGRES_PORT"},
            }
        ),
        "gcs_io_manager": GCSPickleIOManager(
            gcs_bucket=os.getenv("GCP_BUCKET_NAME"),
            gcs=GCSResource(project=os.getenv("GCP_PROJECT_NAME")),
        ),
        "bigquery_io_manager": BigQueryPandasIOManager(
            project=os.getenv("GCP_PROJECT_NAME"),
            dataset="yellow_taxi",
        ),
        "postgres_resource": postgres_resource,
    },
)
