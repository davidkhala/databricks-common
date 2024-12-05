import pathlib
import urllib

from databricks.sdk.runtime import spark, display

from workspace import Workspace
from workspace.catalog import Catalog, Schema

schema = 'nyctlc'


def prepare(catalog):
    _schema = schema
    if catalog:
        Catalog(Workspace()).create(catalog)
        _schema = f"{catalog}.{schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_schema}")


def load_raw(year='2024', month='09', catalog: str = None, volume: str = None):
    tables = ['yellow', 'green', 'fhv', 'fhvhv']
    _dir = "/tmp"

    prepare(catalog)

    if volume and catalog:
        _dir = f"/Volumes/{catalog}/{schema}/{volume}"
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

    for table in tables:
        parquet_file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{table}_tripdata_{year}-{month}.parquet"
        parquet_file_path = f"{_dir}/{table}_tripdata_{year}-{month}.parquet"
        if not pathlib.Path(parquet_file_path).exists():
            urllib.request.urlretrieve(parquet_file_url, parquet_file_path)
        df = spark.read.parquet(parquet_file_path)

        full_name = f"{schema}.{table}"
        if catalog:
            full_name = f"{catalog}.{full_name}"
        df.write.saveAsTable(full_name)

        if _dir == "/tmp":
            pathlib.Path(parquet_file_path).unlink(True)


def clear(catalog: str = None):
    Schema(Workspace(), catalog).delete(schema)
    if catalog:
        Catalog(Workspace()).delete(catalog)


def load(catalog: str = None):
    blob_account_name = "azureopendatastorage"
    blob_container_name = "nyctlc"
    blob_relative_paths = ["yellow", "green", "fhv"]

    prepare(catalog)
    for blob_relative_path in blob_relative_paths:
        # abfss cannot work due to missing required config
        # https cannot work
        wasbs_path = f'wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}'

        blob_df = spark.read.parquet(wasbs_path)

        full_name = f"{schema}.{blob_relative_path}"
        if catalog:
            full_name = f"{catalog}.{full_name}"
        blob_df.write.saveAsTable(full_name)
