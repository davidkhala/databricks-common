import unittest
from davidkhala.databricks.connect import DatabricksConnect


class ConfigTestCase(unittest.TestCase):
    def test_ping(self):
        DatabricksConnect.ping(True)

    def test_find_cluster(self):
        from tests.servermore import get
        spark, controller = get()
        controller.start()
        # print(controller.cluster_id)


if __name__ == '__main__':
    unittest.main()
