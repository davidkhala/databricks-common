import unittest
from typing import Optional

from davidkhala.spark.source.stream import sample
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.streaming.query import StreamingQuery

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from davidkhala.databricks.workspace.warehouse import Warehouse
from servermore import get
from stream import to_table, wait_data, clean, wait_warehouse_data


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
        serverless_table = 'rate_stream_next'
        clean(serverless_table, self.w)
        # case 1: sink to table should be OK
        df = sample(self.spark)
        query, _sql = to_table(df, serverless_table, self.w, self.spark)
        query.awaitTermination()
        warehouse = Warehouse(self.w.client).get_one()
        warehouse.start()
        wait_warehouse_data(warehouse, _sql)

        # cleanup
        if not self.spark.is_stopped:
            self.spark.stop()

    def test_sample_on_servermore(self):
        self.servermore()
        table = 'rate_stream'
        clean(table, self.w)
        df = sample(self.spark)
        _, _sql = to_table(df, table, self.w, self.spark)

        wait_data(self.spark, _sql)

        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
