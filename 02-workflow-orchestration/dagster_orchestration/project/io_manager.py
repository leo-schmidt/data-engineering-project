import warnings
from dagster import (
    IOManager,
    InitResourceContext,
    StringSource,
    io_manager,
    OutputContext,
    InputContext,
    ExperimentalWarning,
)
from sqlalchemy import create_engine
import pandas as pd

warnings.filterwarnings("ignore", category=ExperimentalWarning)


class PandasPostgresIOManager(IOManager):
    def __init__(self, connection_string) -> None:
        self.connection_string = connection_string
        self.engine = create_engine(self.connection_string)

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        table_name = context.asset_key.path[-1]
        obj.to_sql(
            table_name,
            self.engine,
            if_exists="replace",
            index=False,
            chunksize=100000,
        )
        context.log.info(f"DataFrame written to table {table_name} in PostgreSQL.")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        table_name = context.asset_key.path[-1]
        df = pd.read_sql_table(table_name, self.engine)
        context.log.info(f"DataFrame loaded from table {table_name} in PostgreSQL.")
        return df


@io_manager(
    config_schema={
        "username": StringSource,
        "password": StringSource,
        "hostname": StringSource,
        "db_name": StringSource,
        "port": StringSource,
    }
)
def pandas_postgres_io_manager(init_context: InitResourceContext):
    username = init_context.resource_config["username"]
    password = init_context.resource_config["password"]
    hostname = init_context.resource_config["hostname"]
    port = init_context.resource_config["port"]
    db_name = init_context.resource_config["db_name"]

    connection_string = (
        f"postgresql://{username}:{password}@{hostname}:{port}/{db_name}"
    )

    return PandasPostgresIOManager(connection_string)
