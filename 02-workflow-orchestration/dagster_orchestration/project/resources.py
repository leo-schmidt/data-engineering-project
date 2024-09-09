import os
from dagster import ResourceDefinition
from sqlalchemy import create_engine, Engine


def create_postgres_engine() -> Engine:
    username = os.getenv("DAGSTER_POSTGRES_USER")
    password = os.getenv("DAGSTER_POSTGRES_PASSWORD")
    hostname = "db"
    port = os.getenv("DAGSTER_POSTGRES_PORT")
    db_name = os.getenv("DAGSTER_POSTGRES_DB")

    connection_string = (
        f"postgresql://{username}:{password}@{hostname}:{port}/{db_name}"
    )
    return create_engine(connection_string)


postgres_resource = ResourceDefinition(
    resource_fn=create_postgres_engine,
    description="Postgres database connection",
)
