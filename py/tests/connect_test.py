import unittest

from davidkhala.databricks.connect import Session, DatabricksConnect
from davidkhala.databricks.workspace import Workspace


class DatabricksConnectTest(unittest.TestCase):
    data = [
        (1, "Alice", 29),
    ]
    columns = ["id", "name", "age"]
    w = Workspace.from_local()
    config = w.config
    @classmethod
    def setUpClass(cls):
        print(cls.config)
        from davidkhala.databricks.local import CONFIG_PATH
        print(CONFIG_PATH)

    def test_from_context(self):
        spark, _ = DatabricksConnect.get()
        spark.sql('select 1')
        spark.createDataFrame(self.data, self.columns)
        #
        spark.stop()

    def test_ping(self):
        DatabricksConnect.ping(True)
        self.assertTrue(True)

    def test_serverless(self):
        spark = DatabricksConnect.from_serverless(self.config)
        spark.sql('select 1')
        spark.createDataFrame(self.data, self.columns)
        #
        d = Session(spark)
        self.assertTrue(d.serverless)
        #
        print(d.conf)
        spark.stop()

    def test_servermore(self):
        from tests.servermore import get
        spark, controller = get(self.w)
        controller.start()
        spark.sql('select 1')
        spark.createDataFrame(self.data, self.columns)
        #
        session = Session(spark)
        self.assertTrue(session.is_servermore(self.config.cluster_id))
        self.assertTrue(session.is_servermore())
        self.assertEqual('Databricks Shell', session.appName)
        # cleanup
        spark.stop()
        controller.stop()
        self.config.cluster_id = None


if __name__ == '__main__':
    unittest.main()
