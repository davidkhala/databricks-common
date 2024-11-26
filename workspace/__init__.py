from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession


class Workspace:
    api_version = '/api/2.0'
    connection = None

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

    def connect(self):
        from spark import DatabricksConnect
        if self.connection:
            self.connection.close()
        self.connection = DatabricksConnect.from_config(self.client.config)

    @property
    def spark(self) -> SparkSession:
        if self.connection is None:
            self.connect()
        return self.connection.spark

    @property
    def cloud(self):
        token: str = self.config.get('token')
        if token is not None:

            if token.startswith('https://adb-') and token.endswith('.azuredatabricks.net'):
                # adb-662901427557763.3.azuredatabricks.net
                return 'azure'
            elif token.startswith('https://dbc-') and token.endswith('.cloud.databricks.com'):
                # dbc-8df7b30e-676a.cloud.databricks.com
                return "aws"
            elif token.endswith('.gcp.databricks.com'):
                # 1105010096141051.1.gcp.databricks.com
                return "gcp"

    @property
    def dbutils(self):
        return self.client.dbutils

    @property
    def api_client(self):
        return APIClient(self.client)

    def disconnect(self):
        if self.connection:
            self.connection.disconnect()


class APIClient:
    def __init__(self, client: WorkspaceClient):
        self.client = client.api_client

    def get(self, route, data=None):
        return self.client.do(method='GET', path=Workspace.api_version + route, body=data)
