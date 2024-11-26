from databricks.connect import DatabricksSession
from databricks.sdk.config import Config
from pyspark.sql import SparkSession


class DatabricksConnect:
    serverless = None

    def __init__(self, *,
                 cluster_id=None, host=None, token=None, serverless=None):
        _builder = DatabricksSession.builder.validateSession(True)
        if cluster_id:
            _builder.clusterId(cluster_id)
        if serverless:
            _builder.serverless(True)
            self.serverless = True
        if host:
            _builder.host(host)
        if token:
            _builder.token(token)
        self.spark: SparkSession = _builder.getOrCreate()

    @staticmethod
    def from_config(config: Config):
        return DatabricksConnect(cluster_id=config.cluster_id, host=config.host, token=config.token)

    def run(self, sqlQuery):
        return self.spark.sql(sqlQuery)

    def createDataFrame(self, *args):
        if self.serverless:
            raise 'createDataFrame in serverless SQL warehouse is not supported'
        return self.spark.createDataFrame(*args)
    def disconnect(self):
        self.spark.stop()