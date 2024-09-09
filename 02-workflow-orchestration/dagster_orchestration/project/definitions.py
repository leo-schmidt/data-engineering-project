from dagster import Definitions, load_assets_from_modules

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
                "hostname": "db",
                "db_name": {"env": "DAGSTER_POSTGRES_DB"},
                "port": {"env": "DAGSTER_POSTGRES_PORT"},
            }
        ),
        "postgres_resource": postgres_resource,
    },
)
