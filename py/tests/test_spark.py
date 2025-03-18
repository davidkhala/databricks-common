import unittest
from typing import Optional

from davidkhala.spark.source.stream import sample
from pyspark.sql.connect.session import SparkSession

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from tests.servermore import get
from tests.stream import to_table, wait_data, clean


class SampleStreamTestCase(unittest.TestCase):
    w = Workspace.from_local()
    controller: Optional[Cluster] = None
    spark: SparkSession
    table = 'rate_stream'

    def servermore(self):
        self.spark, self.controller = get(self.w)
        self.controller.start()

    def serverless(self):
        spark, serverless = DatabricksConnect.get()
        assert serverless
        self.spark = spark

    def test_clean(self):
        self.serverless()
        table = 'rate_stream'
        v = clean(table, self.w)
        v.create()

    def test_sample_on_serverless(self):
        self.serverless()
        clean(self.table, self.w)
        df = sample(self.spark)
        query, _ = to_table(df, self.table, self.w, self.spark)
        query.awaitTermination()
        # FIXME Spark Connect bug? why this is different than in notebook?
        _, _sql = to_table(df, self.table, self.w, self.spark)

        wait_data(self.spark, _sql)

        if not self.spark.is_stopped:
            self.spark.stop()

    def test_sample_on_servermore(self):
        self.servermore()
        clean(self.table, self.w)
        df = sample(self.spark)
        _, _sql = to_table(df, self.table, self.w, self.spark)

        wait_data(self.spark, _sql)

        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
