from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


class DatabricksConnect:
    def __init__(self):
        self.spark: SparkSession = DatabricksSession.builder.validateSession(True).serverless(True).getOrCreate()
