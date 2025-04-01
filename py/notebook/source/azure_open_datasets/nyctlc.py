import pathlib
import warnings

from davidkhala.databricks import is_databricks_notebook
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.catalog import Catalog, Schema
from davidkhala.databricks.workspace.table import Table
from davidkhala.databricks.workspace.volume import Volume
from notebook.source.azure_open_datasets import context

schema = 'nyctlc'


class NycTLC:
    w = Workspace()
    t = Table(w.client)
    overwrite: bool = True

    def __init__(self):
        from databricks.sdk.runtime import spark
        self.spark = spark
        self.schema = schema
        self.catalog = context.catalog

        Catalog(self.w.client).create(self.catalog)
        Schema(self.w.client,schema).create()

    def copy_to_current(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        tables = ["yellow", "green", "fhv"]
        if not is_databricks_notebook():
            warnings.warn(
                "system.access.table_lineage[entity_type==null]: copy_to_current() invoked outside notebook cannot be detected by Microsoft Purview scan",
                UserWarning)

        for table in tables:
            df = self.spark.table(f"{self.catalog}.{schema}.{table}")
            table_name = f"{schema}.{table}"
            if self.overwrite:
                self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            df.write.saveAsTable(table_name)

    def load_raw(self, year='2024', month='09', volume: str = context.default_volume):
        """
        :param year:
        :param month:
        :param volume:
        :return:
        """
        tables = ['yellow', 'green', 'fhv', 'fhvhv']

        catalog = self.catalog

        v = Volume(self.w, volume, schema, catalog)
        self.spark.sql(f"CREATE VOLUME IF NOT EXISTS {v.full_name}")

        for table in tables:
            basename = f"{table}_tripdata_{year}-{month}.parquet"
            parquet_file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{basename}"

            from urllib.request import urlretrieve
            parquet_file_path = f"{v.path}/{basename}"
            if is_databricks_notebook():
                if not pathlib.Path(parquet_file_path).exists():
                    urlretrieve(parquet_file_url, parquet_file_path)
            else:
                # FIXME: databricks\sdk\retries.py", line 59, in wrapper
                ## raise TimeoutError(f'Timed out after {timeout}') from last_err

                if not v.fs.exists(basename):
                    urlretrieve(parquet_file_url, basename)
                    v.fs.upload(basename)

            df = self.spark.read.parquet(parquet_file_path)

            full_name = f"{catalog}.{schema}.{table}"
            if self.overwrite:
                self.t.delete(full_name)
            df.write.saveAsTable(full_name)

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
            Table(self.w.client).delete(full_name)
            blob_df.write.saveAsTable(full_name)
