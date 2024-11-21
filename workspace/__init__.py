from databricks.sdk import WorkspaceClient

api_version = '/api/2.0'


class Workspace:
    def __init__(self):
        self.client = WorkspaceClient()

    def clusters(self):
        return list(self.client.clusters.list())

    @property
    def config(self) -> dict:
        """
        It returns raw token content. TAKE CARE for secret leakage.
        :return: {'host':'https://adb-662901427557763.3.azuredatabricks.net', 'token':'', 'auth_type':'pat'}
        """
        return self.client.config.as_dict()

    @property
    def dbutils(self):
        return self.client.dbutils

    @property
    def api_client(self):
        return APIClient(self.client)


class APIClient:
    def __init__(self, client: WorkspaceClient):
        self.client = client.api_client

    def get(self, route, data=None):
        return self.client.do(method='GET', path=api_version + route, body=data)
