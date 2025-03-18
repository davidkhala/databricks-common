import unittest
from typing import Optional

from davidkhala.spark.source.stream import sample
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.streaming.query import StreamingQuery

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from tests.servermore import get
from tests.stream import to_table, wait_data


class SampleStreamTestCase(unittest.TestCase):
    w = Workspace.from_local()
    controller: Optional[Cluster] = None
    spark: SparkSession

    def servermore(self):
        self.spark, self.controller = get(self.w)
        self.controller.start()

    def serverless(self):
        spark, serverless = DatabricksConnect.get()
        assert serverless
        self.spark = spark

    def test_sample_on_serverless(self):
        self.serverless()

        query, _sql = self.sample_test(self.spark)
        query.awaitTermination() # It will be stopped automatically
        r = self.spark.sql(_sql)
        self.assertEqual(0, r.count())
        if not self.spark.is_stopped:
            self.spark.stop()

    def test_sample_on_servermore(self):
        self.servermore()
        _, _sql = self.sample_test(self.spark)

        wait_data(self.spark, _sql)

        self.spark.stop()

    def sample_test(self, spark)->(StreamingQuery, str):
        df = sample(spark)
        table = 'rate_stream'
        return to_table(df, table, self.w, spark)


if __name__ == '__main__':
    unittest.main()
