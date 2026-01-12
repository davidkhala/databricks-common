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
        } # TODO this is not structured

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
                df = df.drop("bib_entries", "ref_entries")  # Free form columns
                try:
                    df.write.partitionBy("date").saveAsTable(f"{self.catalog}.{self.schema}.{table}", mode="append")

                except Exception as e:
                    print(f"{file.path}/{path_token}")
# /databricks-datasets/COVID/CORD-19/2020-04-03/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-04-03/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-04-10/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-04-10/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-04-24/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-04-24/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-05-02/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-05-02/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-05-20/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-05-20/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-05-28/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-05-28/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-06-04/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-06-04/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-06-18/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-06-18/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-07-06/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-07-06/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-07-16/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-07-16/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-07-31/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-07-31/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-08-03/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-08-03/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-08-14/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-08-14/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-08-18/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-08-18/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-09-01/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-09-01/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-09-12/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-09-12/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-10-21/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-10-21/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-11-09/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-11-09/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2020-12-21/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2020-12-21/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2021-01-26/biorxiv_medrxiv/biorxiv_medrxiv
# /databricks-datasets/COVID/CORD-19/2021-01-26/comm_use_subset/comm_use_subset
# /databricks-datasets/COVID/CORD-19/2021-03-28/biorxiv_medrxiv/biorxiv_medrxiv
#
# /databricks-datasets/COVID/CORD-19/2021-03-28/comm_use_subset/comm_use_subset
