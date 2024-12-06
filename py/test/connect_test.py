import unittest

from syntax.fs import write_json

from py.common import SparkDecorator
from py.connect import DatabricksConnect
from py.workspace import Workspace


class DatabricksConnectTest(unittest.TestCase):
    data = [
        (1, "Alice", 29),
    ]
    columns = ["id", "name", "age"]
    config = Workspace.from_local().config

    def test_from_context(self):
        spark = DatabricksConnect.get()
        spark.sql('select 1')
        spark.createDataFrame(self.data, self.columns)
        #
        spark.stop()

    def test_ping(self):
        DatabricksConnect.ping(True)

    def test_serverless(self):
        spark = DatabricksConnect.from_serverless(self.config)
        spark.sql('select 1')
        spark.createDataFrame(self.data, self.columns)
        #
        d = SparkDecorator(spark)
        self.assertTrue(d.serverless)
        spark.stop()

    def test_servermore(self):
        self.config.cluster_id = '1128-055322-wt0c1o09'
        spark = DatabricksConnect.from_servermore(self.config)
        spark.sql('select 1')
        spark.createDataFrame(self.data, self.columns)
        write_json(spark.conf.getAll, self.config.cluster_id)
        #
        d = SparkDecorator(spark)
        self.assertFalse(d.serverless)
        spark.stop()
        self.config.cluster_id = None


if __name__ == '__main__':
    unittest.main()
