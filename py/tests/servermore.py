from pyspark.sql import SparkSession

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster


def get(w=Workspace()) -> (SparkSession, Cluster):
    controller = Cluster(w)
    assert controller.get_one() is not None
    controller.pollute()
    return DatabricksConnect.from_servermore(w.config), controller
