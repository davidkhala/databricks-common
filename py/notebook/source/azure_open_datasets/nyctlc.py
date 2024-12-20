import pathlib
import urllib

from notebook.connect import SparkWare
from notebook.source.azure_open_datasets import context

from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.catalog import Catalog, Schema

schema = 'nyctlc'


class NycTLC(SparkWare):
    def __init__(self, spark_instance=None):
        super().__init__(spark_instance)
        self.schema = schema
        self.catalog = context.catalog
        Catalog(Workspace()).create(self.catalog)

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")

    def copy_to_current(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        tables = ["yellow", "green", "fhv"]
        for table in tables:
            df = self.spark.table(f"{self.catalog}.{schema}.{table}")
            df.write.mode("overwrite").saveAsTable(f"{schema}.{table}")

    def load_raw(self, year='2024', month='09', volume: str = context.default_volume):
        tables = ['yellow', 'green', 'fhv', 'fhvhv']
        _dir = "/tmp"

        catalog = self.catalog

        if volume:
            _dir = f"/Volumes/{catalog}/{schema}/{volume}"
            self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

        for table in tables:
            parquet_file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{table}_tripdata_{year}-{month}.parquet"
            parquet_file_path = f"{_dir}/{table}_tripdata_{year}-{month}.parquet"
            if not pathlib.Path(parquet_file_path).exists():
                urllib.request.urlretrieve(parquet_file_url, parquet_file_path)
            df = self.spark.read.parquet(parquet_file_path)

            full_name = f"{catalog}.{schema}.{table}"

            df.write.mode("overwrite").saveAsTable(full_name)

            if not volume:
                pathlib.Path(parquet_file_path).unlink(True)

    def load(self):
        """
        Not lineage will be introduced
        :return:
        """
        blob_account_name = "azureopendatastorage"
        blob_container_name = "nyctlc"
        blob_relative_paths = ["yellow", "green", "fhv"]
        catalog = self.catalog
        for blob_relative_path in blob_relative_paths:
            # abfss cannot work due to missing required config
            # https cannot work
            wasbs_path = f'wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}'

            blob_df = self.spark.read.parquet(wasbs_path)

            full_name = f"{catalog}.{schema}.{blob_relative_path}"

            blob_df.write.mode("overwrite").saveAsTable(full_name)
