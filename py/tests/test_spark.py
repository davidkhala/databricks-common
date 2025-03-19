import unittest
from time import sleep
from typing import Optional

from davidkhala.spark.source.stream import sample
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.streaming.query import StreamingQuery

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from davidkhala.databricks.workspace.warehouse import Warehouse
from tests.servermore import get
from tests.stream import to_table, wait_data, clean, wait_warehouse_data


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

    def test_sample_on_serverless(self):
        self.serverless()
        clean(self.table, self.w)
        df = sample(self.spark)

        query: StreamingQuery
        query, _ = to_table(df, self.table, self.w, self.spark)
        query.awaitTermination()

        # FIXME Spark Connect bug? why this is different than in notebook?
        query, _sql = to_table(df, self.table, self.w, self.spark)
        query.awaitTermination()

        warehouse = Warehouse(self.w.client).get_one()
        warehouse.start()
        wait_warehouse_data(warehouse, _sql)

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
