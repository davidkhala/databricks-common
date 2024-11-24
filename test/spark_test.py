import unittest

from databricks.connect import DatabricksSession  # type: ignore

from spark import DatabricksConnect


class Connect(unittest.TestCase):
    def setUp(self):
        spark = DatabricksConnect().spark
        s = spark.getActiveSession()
        self.assertIsNotNone(s)
        self.spark = spark
    def test_create_DF(self):

        data = [
            (1, "Alice", 29),
            (2, "Bob", 31),
            (3, "Cathy", 25)
        ]

        # Define schema (column names)
        columns = ["id", "name", "age"]
        self.spark.createDataFrame(data, columns)



if __name__ == '__main__':
    unittest.main()
