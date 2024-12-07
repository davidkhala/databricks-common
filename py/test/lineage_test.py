import unittest

from davidkhala.syntax.fs import write_json

from davidkhala.databricks.lineage.rest import API as RESTAPI
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.path import SDK as PATHSDK
from davidkhala.databricks.workspace.table import Table
from davidkhala.databricks.connect.notebook import Index as NotebookIndex

w = Workspace.from_local()


class TestRest(unittest.TestCase):
    def setUp(self):
        self.api = RESTAPI(w.api_client)
        self.t = Table(w.client)

    def test_API_lineage(self):
        table_name = 'azure-open-datasets.nyctlc.yellow'
        table_lineage = self.api.get_table(table_name)

        write_json(table_lineage, table_name + '.table')
        # column lineage
        columns = self.t.columns(table_name)
        for column in columns:
            c_l = self.api.get_column(table_name, column)
            print(c_l)




class TestE2E(unittest.TestCase):
    s = PATHSDK.from_workspace(w)
    def setUp(self):
        i = NotebookIndex(w)
    def test_start(self):
        r = self.s.get_by(notebook_id=918032188629039)
        print(r)


if __name__ == '__main__':
    unittest.main()
