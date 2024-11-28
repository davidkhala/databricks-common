import unittest

from databricks.connect import DatabricksSession  # type: ignore
from syntax.fs import write_json

from common import DatabricksConnect, CONFIG_PATH, logout
from workspace import Workspace


class TestDatabricksConnect(unittest.TestCase):
    data = [
        (1, "Alice", 29),
    ]
    columns = ["id", "name", "age"]
    config = Workspace().config

    def setUp(self):
        print(self.config)
        print(CONFIG_PATH)

    def test_from_context(self):
        spark = DatabricksConnect.get()
        spark.sql('select 1')
        spark.createDataFrame(self.data, self.columns)
        spark.stop()

    def test_serverless(self):
        spark = DatabricksConnect.from_serverless(self.config)
        spark.sql('select 1')
        spark.createDataFrame(self.data, self.columns)
        d = DatabricksConnect(spark)
        self.assertTrue(d.serverless)
        spark.stop()

    def test_servermore(self):
        self.config.cluster_id = '1128-055322-wt0c1o09'
        spark = DatabricksConnect.from_servermore(self.config)
        spark.sql('select 1')
        spark.createDataFrame(self.data, self.columns)
        write_json(spark.conf.getAll, self.config.cluster_id)
        d = DatabricksConnect(spark)
        self.assertFalse(d.serverless)
        spark.stop()
        self.config.cluster_id = None


if __name__ == '__main__':
    unittest.main()
