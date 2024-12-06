import unittest

class TestCase(unittest.TestCase):
    def test_ping(self):
        from py.connect import DatabricksConnect
        DatabricksConnect.ping(True)



if __name__ == '__main__':
    unittest.main()
