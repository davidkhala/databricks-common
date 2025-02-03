import unittest
from typing import Optional

from davidkhala.spark.source.stream import sample
from pyspark.sql import DataFrame
from pyspark.sql.connect.session import SparkSession

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from davidkhala.databricks.workspace.table import Table
from davidkhala.databricks.workspace.volume import Volume
from tests.servermore import get


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

        r = self.test_sample(self.spark, True)
        self.assertEqual(0, r.count())

    def test_sample_on_servermore(self):
        self.servermore()
        r = self.test_sample(self.spark, False)
        self.assertGreater(r.count(), 0)

    def test_sample(self, spark, serverless):
        df = sample(spark)
        from davidkhala.databricks.sink.stream import Internal
        i = Internal(df, serverless)
        table = 'rate_stream'
        Table(self.w.client).delete(f"{self.w.catalog}.default.{table}")
        volume = Volume(self.w, table)
        volume.delete()
        query = i.toTable(table, volume)
        query.awaitTermination(10)
        r: DataFrame = spark.sql('select * from ' + table)
        return r

    def tearDown(self):
        if self.controller:
            self.controller.stop()
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
