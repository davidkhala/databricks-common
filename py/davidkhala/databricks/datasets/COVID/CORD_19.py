from databricks.sdk import WorkspaceClient
from pyspark.sql import functions
from pyspark.sql.connect.session import SparkSession

from davidkhala.databricks.datasets import Loader as BaseLoader
from davidkhala.databricks.datasets.COVID import catalog
from davidkhala.databricks.dbutils import FS


class Loader(BaseLoader):
    def __init__(self, w: WorkspaceClient, spark: SparkSession):
        table_path_map = {
            'biorxiv_medrxiv': 'biorxiv_medrxiv/biorxiv_medrxiv',
            'comm_use_subset': 'comm_use_subset/comm_use_subset'
        }

        super().__init__(w, spark, table_path_map,
                         catalog=catalog,
                         schema='CORD_19',
                         schema_path='CORD-19'
                         )
        self.fs = FS(self.w.dbutils)

    def start(self):
        self.c.create(self.catalog)
        self.s.create()
        relative_path = f"/databricks-datasets/{self.catalog_path}/{self.schema_path}"
        dirs = self.fs.ls(relative_path, ignore_patterns=[*FS.ignore, '.csv'])
        for file in dirs:
            assert FS.isDir(file)

            date_str = file.path[-10:]
            for table, path_token in self.path_map.items():
                df = (self.spark.read
                      .option("multiLine", True)
                      .json(f"{file.path}/{path_token}")
                      .withColumn("date", functions.to_date(functions.lit(date_str), "yyyy-MM-dd"))
                      )
                df = df.drop("bib_entries", "ref_entries")
                (df.write
                 .partitionBy("date")
                 .saveAsTable(f"{self.catalog}.{self.schema}.{table}", mode="append")
                 )
