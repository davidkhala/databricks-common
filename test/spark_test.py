import unittest

from databricks.connect import DatabricksSession  # type: ignore

from spark import DatabricksConnect
from workspace import Workspace


class TestDatabricksConnect(unittest.TestCase):
    data = [
        (1, "Alice", 29),
    ]
    columns = ["id", "name", "age"]
    config = Workspace().config

    def test_create_DF(self):
        connect = DatabricksConnect()
        connect.createDataFrame(self.data, self.columns)
        connect.disconnect()

    def test_query(self):
        connect = DatabricksConnect()
        connect.run('select 1 ')
        connect.disconnect()

    def test_warehouse_query(self):
        connect = DatabricksConnect()
        connect.run('select 1')
        with self.assertRaises(Exception):
            connect.createDataFrame(self.data, self.columns)
        connect.disconnect()


if __name__ == '__main__':
    unittest.main()
