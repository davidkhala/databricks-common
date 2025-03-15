from pyspark.sql import SparkSession

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster


class Controller(Cluster):
    def start(self):
        print(f"cluster [{self.cluster_id}] starting...")
        super().start()
        print(f"cluster [{self.cluster_id}] started")


def get(w=Workspace()) -> (SparkSession, Controller):
    controller = Controller(w.client)
    assert controller.as_one() is not None
    return DatabricksConnect.from_servermore(w.config), controller
