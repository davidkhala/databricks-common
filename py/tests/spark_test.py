import unittest
from typing import Optional

from davidkhala.spark.source.stream import sample
from pyspark.sql.connect.session import SparkSession

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from tests.servermore import get
from tests.stream import to_table, tearDown


class SampleStreamTestCase(unittest.TestCase):
    w = Workspace.from_local()
    controller: Optional[Cluster]
    spark: SparkSession

    def servermore(self):
        self.spark, self.controller = get(self.w)
        self.controller.start()

    def serverless(self):
        spark, serverless = DatabricksConnect.get()
        assert serverless
        self.spark = spark

    def setUp(self):
        self.controller = None

    def test_sample_on_serverless(self):
        self.serverless()

        r = self.sample_test(self.spark)
        self.assertEqual(0, r.count())

    def test_sample_on_servermore(self):
        self.servermore()
        r = self.sample_test(self.spark)
        self.assertGreater(r.count(), 0)

    def sample_test(self, spark):
        df = sample(spark)
        table = 'rate_stream'
        return to_table(df, table, self.w, spark)

    def tearDown(self):
        tearDown(self.spark, self.controller)


if __name__ == '__main__':
    unittest.main()
