import unittest
from databricks.sdk import WorkspaceClient
from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace.server import Cluster


class ConfigTestCase(unittest.TestCase):
    def test_ping(self):
        DatabricksConnect.ping(True)

    def test_find_cluster(self):
        w = WorkspaceClient()
        c = Cluster(w)
        c.as_one()
        self.assertIsNotNone(c.cluster_id)
        print(c.cluster_id)
        c.start()


if __name__ == '__main__':
    unittest.main()
