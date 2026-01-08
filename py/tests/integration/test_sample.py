import unittest

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.dbutils import FS
w = Workspace.from_local()

class DBUtils(unittest.TestCase):
    dbutils = w.dbutils
    fs = FS(dbutils)
    def test_ls(self):
        for file_info in self.dbutils.fs.ls("/databricks-datasets"):
            print(file_info)
    def test_load(self):

        spark, serverless = DatabricksConnect.get()
        print('serverless', serverless)
        for file_info in self.dbutils.fs.ls("/databricks-datasets"):
            sample_file = file_info.path

            if sample_file.endswith(".csv"):
                df = spark.read.option("header", True).csv(sample_file)
            elif sample_file.endswith(".json"):
                df = spark.read.json(sample_file)
            elif sample_file.endswith(".parquet"):
                df = spark.read.parquet(sample_file)
            # df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table}")

    def test_traverse(self):
        for file_info in self.dbutils.fs.ls("/databricks-datasets"):
            path = file_info.path
            if path.endswith(".md"):
                print(f"cat {path}")
                # print(self.dbutils.fs.head(path))
            else:
                print(f"ls {path}")
                print(self.dbutils.fs.ls(path))


    def test_type(self):
        print(type(self.dbutils))
    def test_cat(self):
        self.fs.cat("/databricks-datasets/README.md")

    def test_COVID(self):

        targets = [
            'colleges', 'excess-deaths', 'live', 'mask-use'
        ]
        for file_info in self.fs.ls("/databricks-datasets/COVID/covid-19-data"):
            print(file_info.path)

        # subgroup
        for target in targets:
            print(f"|- {target}")
            for file_info in self.fs.ls(f"/databricks-datasets/COVID/covid-19-data/{target}"):
                print(file_info.path)

if __name__ == '__main__':
    unittest.main()
