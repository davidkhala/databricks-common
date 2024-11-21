import unittest
from lineage.table import TableLineage
from workspace import Workspace


class TestLineage(unittest.TestCase):
    def setUp(self):
        warehouse = '/sql/1.0/warehouses/7969d92540da7f02'
        self.t = TableLineage(Workspace().client, warehouse)

    def test_table_lineage(self):
        l = self.t.all()
        print(l)


if __name__ == '__main__':
    unittest.main()
