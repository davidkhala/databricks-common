from databricks.sdk import WorkspaceClient
from davidkhala.spark.source.file import read
from pyspark.sql.connect.session import SparkSession

from davidkhala.databricks.workspace.catalog import Catalog, Schema


class Loader:

    def __init__(self, w: WorkspaceClient, spark: SparkSession,table_path_map:dict, *, catalog: str, schema: str):
        self.w = w
        self.spark = spark
        self.c = Catalog(w)
        self.s = Schema(w, schema, catalog)
        self.catalog = catalog
        self.schema = schema
        self.path_map = table_path_map
    def start(self):
        self.c.create(self.catalog)
        self.s.create()
        for table, path in self.path_map.items():
            read(self.spark, f"/databricks-datasets/{self.catalog}/{self.schema}/{path}")
        # load table if not exist, you need to use spark dataframe