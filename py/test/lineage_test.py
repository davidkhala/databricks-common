import unittest

from davidkhala.syntax.fs import write_json

from davidkhala.databricks.lineage.rest import API as RESTAPI
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.table import Table

w = Workspace.from_local()


class TestRest(unittest.TestCase):
    def setUp(self):
        self.api = RESTAPI(w.api_client)
        self.t = Table(w.client)

    def test_API_lineage(self):
        table_name = 'azureopendatastorage.nyctlc.yellow'
        table_lineage = self.api.get_table(table_name)

        write_json(table_lineage, table_name + '.lineage')
        # column lineage
        columns = self.t.column_names(table_name)
        for column in columns:
            c_l = self.api.get_column(table_name, column)
            print(c_l)


if __name__ == '__main__':
    unittest.main()
