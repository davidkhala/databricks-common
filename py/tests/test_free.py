# testing on free edition
import unittest


class HotmailTestCase(unittest.TestCase):
    workspace = 'https://dbc-b9e7e6ee-7e63.cloud.databricks.com/'
    token = 'dapi'+'036089e12de676f44e7f5b1510fdd5e1'
    data = [
        (1, "Alice", 29),
    ]
    columns = ["id", "name", "age"]
    def test_ping(self):
        from databricks.sdk.config import Config
        from davidkhala.databricks.connect import DatabricksConnect
        from davidkhala.databricks.connect import Session
        config =  Config(host = self.workspace, token = self.token)
        spark = DatabricksConnect.from_serverless(config)

        spark.sql('select 1')
        df = spark.createDataFrame(self.data, self.columns)
        print(df)
        #
        d = Session(spark)
        self.assertTrue(d.serverless)
        #
        print(d.conf)
        spark.stop()





if __name__ == '__main__':
    unittest.main()
