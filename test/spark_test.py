import unittest

from databricks.connect import DatabricksSession  # type: ignore

class ContextTest(unittest.TestCase):
    def test_get_singleton(self):
        spark = DatabricksSession.builder.validateSession(True).serverless(True).getOrCreate()
        print(type(spark))
        s= spark.active()
        print(type(s))



if __name__ == '__main__':
    unittest.main()
