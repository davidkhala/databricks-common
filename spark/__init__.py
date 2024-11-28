from databricks.connect import DatabricksSession
from databricks.sdk.config import Config
from pyspark.sql import SparkSession


class DatabricksConnect:
    spark: SparkSession

    def __init__(self, spark):

        self.spark: SparkSession = spark

    @staticmethod
    def get()->SparkSession:
        return DatabricksSession.builder.validateSession(True).getOrCreate()

    @staticmethod
    def from_config(config: Config)->SparkSession:
        _builder = DatabricksSession.builder.validateSession(True)
        _builder.host(config.host)
        _builder.token(config.token)
        _builder.clusterId(config.cluster_id)
        return _builder.getOrCreate()

    def disconnect(self):
        self.spark.stop()
