import unittest

class TestCase(unittest.TestCase):
    def test_ping(self):
        from davidkhala.databricks.connect import DatabricksConnect
        DatabricksConnect.ping(True)



if __name__ == '__main__':
    unittest.main()
