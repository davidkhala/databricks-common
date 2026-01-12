from databricks.sdk import WorkspaceClient
from davidkhala.spark.source.file import read
from pyspark.sql.connect.session import SparkSession

from davidkhala.databricks.workspace.catalog import Catalog, Schema


class Loader:

    def __init__(self, w: WorkspaceClient, spark: SparkSession, table_path_map: dict,
                 *,
                 catalog: str,
                 schema: str,
                 schema_path=None,
                 catalog_path=None
                 ):
        self.w = w
        self.spark = spark
        self.c = Catalog(w)
        self.s = Schema(w, schema, catalog)
        self.catalog = catalog
        self.catalog_path = catalog_path or catalog
        self.schema = schema
        self.schema_path = schema_path or schema
        self.path_map = table_path_map

    def start(self):
        self.c.create(self.catalog)
        self.s.create()
        for table, path in self.path_map.items():
            df = read(self.spark, f"/databricks-datasets/{self.catalog_path}/{self.schema_path}/{path}")
            df.write.saveAsTable(f"{self.catalog}.{self.schema}.{table}", mode="overwrite")


    def delete(self):
        self.s.delete()
        self.c.delete(self.catalog)