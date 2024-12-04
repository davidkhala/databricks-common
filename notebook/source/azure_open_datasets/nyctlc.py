import urllib

from databricks.sdk.runtime import spark, display

schema = 'nyctlc'


def load_http(year='2024', month='09', catalog: str = None, volume: str = None):
    tables = ['yellow', 'green', 'fhv', 'fhvhv']
    _dir = "/tmp"

    if catalog:
        from workspace import Workspace
        from workspace.catalog import Catalog
        Catalog(Workspace()).create(catalog)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    if volume and catalog:
        _dir = f"/Volumes/{catalog}/{schema}/{volume}"
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

    for table in tables:
        parquet_file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{table}_tripdata_{year}-{month}.parquet"
        parquet_file_path = f"{_dir}/{table}_tripdata_{year}-{month}.parquet"
        urllib.request.urlretrieve(parquet_file_url, parquet_file_path)
        df = spark.read.parquet(parquet_file_path)

        full_name = f"{schema}.{table}"
        if catalog:
            full_name = f"{catalog}.{full_name}"
        df.write.saveAsTable(full_name)

def load():

    # Azure Blob Storage access info
    blob_account_name = "azureopendatastorage"
    blob_container_name = "nyctlc"
    blob_relative_paths = ["yellow"]

    for blob_relative_path in blob_relative_paths:

        # abfss cannot work due to missing required config
        # https cannot work
        wasbs_path = f'wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}'

        # Read parquet data from Azure Blob Storage path
        blob_df = spark.read.parquet(wasbs_path)
        display(blob_df)

