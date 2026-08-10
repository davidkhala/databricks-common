import unittest
from typing import Optional

from davidkhala.spark.source.stream import sample
from pyspark.sql.connect.session import SparkSession

from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from servermore import get
from stream import to_table, wait_data, clean


class SampleStreamTestCase(unittest.TestCase):
    w = Workspace.from_local()
    controller: Optional[Cluster] = None
    spark: SparkSession

    def servermore(self):
        self.spark, self.controller = get(self.w)
        self.controller.start()

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
