from databricks.sdk import WorkspaceClient


class ClientWare:
    client: WorkspaceClient
    def __init__(self, client: WorkspaceClient):
        self.client = client