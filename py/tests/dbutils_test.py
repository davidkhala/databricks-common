import unittest

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.dbutils import FS
from davidkhala.databricks.workspace import Workspace

dbutils = Workspace.from_local().dbutils
class FSTest(unittest.TestCase):
    fs = FS(dbutils)
    def test_tree(self):
        self.fs.tree("/databricks-datasets/COVID/covid-19-data")
    def test_ls(self):
        for file in self.fs.ls("/databricks-datasets"):
            print(file)
    def test_cat(self):
        self.fs.cat("/databricks-datasets/README.md")
    def test_explore(self):
        # for file in self.fs.ls("/databricks-datasets/COVID/CORD-19"):
        #     print(file)
        for file in self.fs.ls("/databricks-datasets/COVID/CORD-19/2020-03-13"):
            print(file)
        self.fs.cat('/databricks-datasets/COVID/CORD-19/2020-03-13/json_schema.txt')
        self.fs.cat('/databricks-datasets/COVID/CORD-19/2020-03-13/all_sources_metadata_2020-03-13.readme')
    def test_explore3(self):
        l = self.fs.ls("/databricks-datasets/COVID/CORD-19/2020-04-03/biorxiv_medrxiv/biorxiv_medrxiv")
        spark, _ = DatabricksConnect.get()
        for file in l:
            df = spark.read.option("multiLine", True). json(file.path)
            if "_corrupt_record" in df.columns:
                print(file.path)
                self.fs.cat(file.path)
                break
            else:
                df.printSchema()
                break

    def test_explore4(self):
        l = self.fs.ls("/databricks-datasets/COVID/CORD-19/2020-04-03/biorxiv_medrxiv/biorxiv_medrxiv/pdf_json")
        for file in l:
            print(file)
        print('--line break--')
        l = self.fs.ls("/databricks-datasets/COVID/CORD-19/2020-12-21")
        for file in l:
            print(file)