from sqlalchemy import create_engine
import os
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

host = os.environ.get("PG_HOST")
name = os.environ.get("PG_NAME")
username = os.environ.get("PG_USER")
password = os.environ.get("PG_PASSWORD")
port = os.environ.get("PG_PORT")


def load_parquet(path, table_name):
    engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{name}")

    file = pq.ParquetFile(path)
    df = next(file.iter_batches(batch_size=10)).to_pandas()
    df_iter = file.iter_batches(batch_size=100000)

    df.head(0).to_sql(table_name, engine, if_exists="replace")

    batch_count = 0
    for batch in df_iter:
        print(f"Saving batch {batch_count+1}")
        df = batch.to_pandas()
        df.to_sql(table_name, engine, if_exists="append")
        batch_count += 1


if __name__ == "__main__":
    load_parquet(
        "./01_docker_terraform/yellow_tripdata_2024-01.parquet",
        "tripdata",
    )
