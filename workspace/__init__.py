from databricks.sdk import WorkspaceClient


class Workspace:
    def __init__(self):
        self.client = WorkspaceClient()

    def clusters(self):
        return list(self.client.clusters.list())

    @property
    def dbutils(self):
        return self.client.dbutils