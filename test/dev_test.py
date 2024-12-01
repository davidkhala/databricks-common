import unittest

import pyspark

from workspace import Workspace


class DevTestCase(unittest.TestCase):
    def test_something(self):
        import urllib
        urllib.request.urlretrieve(
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-09.parquet",
            "/tmp/yellow_tripdata_2024-09.parquet")

    def test_volume(self):
        dbutils = Workspace().dbutils
        try:
            dbutils.fs.ls('/Volumes/az_databricks/default/volume')
        except pyspark.errors.exceptions.captured.AnalysisException as e:
            if str(e).startswith('[UC_VOLUME_NOT_FOUND]'):
                return


if __name__ == '__main__':
    unittest.main()
