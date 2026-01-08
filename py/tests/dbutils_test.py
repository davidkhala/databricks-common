import unittest

from davidkhala.databricks.dbutils import FS
from davidkhala.databricks.workspace import Workspace

dbutils = Workspace.from_local().dbutils
class FSTest(unittest.TestCase):
    fs = FS(dbutils)
    def test_tree(self):
        self.fs.tree("/databricks-datasets/COVID")
    def test_ls(self):
        for file in self.fs.ls("/databricks-datasets/COVID/covid-19-data"):
            print(file)

