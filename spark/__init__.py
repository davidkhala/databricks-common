from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


class DatabricksConnect:
    def __init__(self):
        """
        # https://learn.microsoft.com/zh-cn/azure/databricks/dev-tools/databricks-connect/python/install#validate
        """
        self.spark: SparkSession = DatabricksSession.builder.validateSession(True).serverless(True).getOrCreate()
