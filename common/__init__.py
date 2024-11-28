from databricks.connect import DatabricksSession
from databricks.sdk.config import Config
from pyspark.sql import SparkSession
from syntax.path import home_resolve


class DatabricksConnect:
    spark: SparkSession

    def __init__(self, spark):
        self.spark: SparkSession = spark

    @staticmethod
    def get() -> SparkSession:
        return DatabricksSession.builder.validateSession(True).getOrCreate()

    @staticmethod
    def from_servermore(config: Config) -> SparkSession:
        _builder = DatabricksSession.builder.validateSession(True)
        _builder.host(config.host)
        _builder.token(config.token)
        _builder.clusterId(config.cluster_id)
        return _builder.getOrCreate()

    @staticmethod
    def from_serverless(config: Config) -> SparkSession:
        # session validation flag is in vain. It will be overwritten
        # > Disable session validation for serverless
        _builder = DatabricksSession.builder
        _builder.host(config.host)
        _builder.token(config.token)
        _builder.serverless(True)
        return _builder.getOrCreate()

    def disconnect(self):
        self.spark.stop()

    @property
    def schema(self) -> str:
        """
        :return: current schema full name
        """
        return self.spark.catalog.currentCatalog() + '.' + self.spark.catalog.currentDatabase()

    @property
    def serverless(self) -> bool:
        return self.conf.get("spark.databricks.clusterUsageTags.clusterId") is None

    @property
    def conf(self) -> dict:
        return self.spark.conf.getAll


CONFIG_PATH = home_resolve('.databrickscfg')
import pathlib


def logout():
    pathlib.Path(CONFIG_PATH).unlink(True)
