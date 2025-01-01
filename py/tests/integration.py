import unittest

from davidkhala.syntax.fs import write_json

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.table import Table

w = Workspace.from_local()

class TableTest(unittest.TestCase):
    def setUp(self):
        self.t = Table(w.client)
        self.spark = DatabricksConnect.get()
        from notebook.source.azure_open_datasets.nyctlc import NycTLC

        instance = NycTLC(self.spark)
        instance.load()
        instance.copy_to_current()

    def test_table_get(self):
        from notebook.source.azure_open_datasets.context import catalog
        table_name = f"{catalog}.nyctlc.yellow"
        r = self.t.get(table_name)
        write_json(r, table_name)
    def tearDown(self):
        self.spark.stop()

class LineageTest(unittest.TestCase):
    def setUp(self):
        from davidkhala.databricks.lineage.rest import API as RESTAPI
        self.api = RESTAPI(w.api_client)
        self.t = Table(w.client)

    def test_API_lineage(self):
        from notebook.source.azure_open_datasets.context import catalog
        table_name = f"{catalog}.nyctlc.yellow"
        table_lineage = self.api.get_table(table_name)

        write_json(table_lineage, table_name + '.lineage')
        # column lineage
        columns = self.t.column_names(table_name)
        for column in columns:
            c_l = self.api.get_column(table_name, column)
            print(c_l)



if __name__ == '__main__':
    unittest.main()
