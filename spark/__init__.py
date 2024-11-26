from databricks.connect import DatabricksSession
from databricks.sdk.config import Config
from pyspark.sql import SparkSession


class DatabricksConnect:
    def __init__(self, cluster_id: str = None):
        _builder = DatabricksSession.builder.validateSession(True)
        if cluster_id:
            _builder.clusterId(cluster_id)
        else:
            _builder.serverless(True)
            self.serverless = True

        self.spark: SparkSession = _builder.getOrCreate()

    @staticmethod
    def from_config(config: Config):
        return DatabricksConnect(config.cluster_id)

    def run(self, sqlQuery):
        return self.spark.sql(sqlQuery)

    def createDataFrame(self, *args):
        if self.serverless:
            raise 'createDataFrame in serverless SQL warehouse is not supported'
        return self.spark.createDataFrame(*args)
