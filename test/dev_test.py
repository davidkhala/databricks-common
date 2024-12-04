import unittest

import pyspark

from common import DatabricksConnect
from workspace import Workspace

parquet_in_http = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-09.parquet"
class DevTestCase(unittest.TestCase):
    def setUp(self):
        self.spark = DatabricksConnect.get()
    def test_something(self):

        self.spark.sparkContext.addFile(parquet_in_http)





if __name__ == '__main__':
    unittest.main()
