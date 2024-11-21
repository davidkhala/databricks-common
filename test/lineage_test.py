import unittest

from syntax.fs import write_json

from lineage.table import TableLineage
from lineage.rest import API
from workspace import Workspace

w = Workspace()


class TestAPI(unittest.TestCase):
    def setUp(self):
        self.api = API(w.api_client)

    def test_API_lineage(self):
        table_name = 'azure-open-datasets.nyctlc.yellow'
        table_lineage = self.api.get_table(table_name)
        print(table_lineage)


class TestSDK(unittest.TestCase):
    def setUp(self):
        warehouse = '/sql/1.0/warehouses/7969d92540da7f02'
        self.t = TableLineage(w.client, warehouse)

    def test_table_lineage(self):
        l = self.t.all()
        write_json(l, "table-lineage")


if __name__ == '__main__':
    unittest.main()
