import unittest

from databricks.connect import DatabricksSession  # type: ignore

from spark import DatabricksConnect


class TestDatabricksConnect(unittest.TestCase):
    def setUp(self):
        connect = DatabricksConnect()
        s = connect.spark.getActiveSession()
        self.assertIsNotNone(s)

    def test_create_DF(self):
        connect = DatabricksConnect(serverless=True)
        data = [
            (1, "Alice", 29),
        ]
        columns = ["id", "name", "age"]
        with self.assertRaises(Exception):
            connect.createDataFrame(data, columns)

        connect = DatabricksConnect()
        connect.createDataFrame(data, columns)

    def test_query(self):
        connect = DatabricksConnect()
        connect.run('select 1 ')


if __name__ == '__main__':
    unittest.main()
