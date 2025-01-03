import unittest

from davidkhala.databricks.connect import SessionDecorator, DatabricksConnect
from davidkhala.databricks.workspace import Workspace


class DatabricksConnectTest(unittest.TestCase):
    data = [
        (1, "Alice", 29),
    ]
    columns = ["id", "name", "age"]
    w = Workspace.from_local()
    config = w.config

    def setUp(self):
        print(self.config)
        from davidkhala.databricks.local import CONFIG_PATH
        print(CONFIG_PATH)

    def test_from_context(self):
        spark = DatabricksConnect.get()
        spark.sql('select 1')
        spark.createDataFrame(self.data, self.columns)
        #
        spark.stop()

    @staticmethod
    def test_ping():
        DatabricksConnect.ping(True)

    def test_serverless(self):
        spark = DatabricksConnect.from_serverless(self.config)
        spark.sql('select 1')
        spark.createDataFrame(self.data, self.columns)
        #
        d = SessionDecorator(spark)
        self.assertTrue(d.serverless)
        #
        print(d.conf)
        spark.stop()

    def test_servermore(self):
        clusters = self.w.cluster_ids()

        if len(clusters) > 0:
            cluster_id = clusters[0]
            print(cluster_id)
            self.config.cluster_id = cluster_id
            from davidkhala.databricks.workspace.server import Cluster
            Cluster(self.w.client, cluster_id).start()

            spark = DatabricksConnect.from_servermore(self.config)
            spark.sql('select 1')
            spark.createDataFrame(self.data, self.columns)
            #
            d = SessionDecorator(spark)
            self.assertTrue(d.is_servermore(self.config.cluster_id))
            # cleanup
            spark.stop()
            Cluster(self.w.client, cluster_id).stop()
            self.config.cluster_id = None


if __name__ == '__main__':
    unittest.main()
