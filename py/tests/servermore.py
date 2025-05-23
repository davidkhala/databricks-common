from pyspark.sql.connect.session import SparkSession

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
    controller.as_one()
    assert controller.cluster_id is not None
    controller.pollute()
    return DatabricksConnect.from_servermore(w.config), controller
