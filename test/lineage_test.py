import unittest

from syntax.fs import write_json

from lineage import Table as TableLineage, Column as ColumnLineage
from lineage.rest import API as RESTAPI
from workspace import Workspace
from workspace.path import SDK as PATHSDK, NotebookIndex
from workspace.table import Table
from workspace.warehouse import Warehouse

w = Workspace()


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


class TestLineage(unittest.TestCase):
    def setUp(self):
        http_path = '/sql/1.0/warehouses/f74f8ec14f4e81fa'
        warehouse = Warehouse(w.client, http_path)
        self.t = TableLineage(w.client)
        self.c = ColumnLineage(w.client, warehouse)

    def test_table_lineage(self):
        l = self.t.run()
        write_json(l, "all-table-lineage")

    def test_column_lineage(self):
        c = self.c.run()
        write_json(c, "all-column-lineage")


class TestE2E(unittest.TestCase):
    s = PATHSDK.from_workspace(w)
    def setUp(self):
        i = NotebookIndex(w)
    def test_start(self):
        r = self.s.get_by(notebook_id=918032188629039)
        print(r)


if __name__ == '__main__':
    unittest.main()
