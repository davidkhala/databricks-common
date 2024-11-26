import unittest

from databricks.connect import DatabricksSession  # type: ignore

from spark import DatabricksConnect


class TestDatabricksConnect(unittest.TestCase):
    def setUp(self):
        self.spark = DatabricksConnect().spark
        s = self.spark.getActiveSession()
        self.assertIsNotNone(s)

    def test_create_DF(self):
        with self.assertRaises(Exception):
            data = [
                (1, "Alice", 29),
            ]
            columns = ["id", "name", "age"]
            self.spark.createDataFrame(data, columns)

    def test_query(self):
        self.spark.sql('select 1 ')


if __name__ == '__main__':
    unittest.main()
