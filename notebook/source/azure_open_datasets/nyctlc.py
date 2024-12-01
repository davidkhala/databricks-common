import urllib

from databricks.sdk.runtime import spark

schema = 'nyctlc'


def load(year='2024', month='09', catalog: str = None, volume: str = None):
    tables = ['yellow', 'green', 'fhv', 'fhvhv']
    _dir = "/tmp"

    if catalog:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"USE {catalog}")
        # TODO Metastore storage root URL does not exist. Please provide a storage location for the catalog
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
