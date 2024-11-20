from databricks.sdk import WorkspaceClient


class Cluster:
    def __init__(self):
        self.client = WorkspaceClient()

    def list(self):
        return self.client.clusters.list()
