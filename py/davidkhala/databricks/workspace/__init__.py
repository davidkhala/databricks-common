import pathlib
from typing import Union

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.core import ApiClient
from databricks.sdk.dbutils import RemoteDbUtils
from databricks.sdk.service.iam import User


class Workspace:
    client: WorkspaceClient

    def __init__(self, client: WorkspaceClient = None):
        if client is None:
            client = WorkspaceClient()
        self.client = client

    @staticmethod
    def from_local():
        from davidkhala.databricks.local import CONFIG_PATH
        if not pathlib.Path(CONFIG_PATH).exists():
            raise FileNotFoundError(CONFIG_PATH + " does not exist")
        return Workspace()

    @property
    def config(self) -> Config:
        """
        It returns raw token content. TAKE CARE for secret leakage.
        :return: {'host':'https://adb-662901427557763.3.azuredatabricks.net', 'token':'...', 'auth_type':'pat'}
        """
        return self.client.config

    @property
    def config_token(self)->str:
        return self.config.as_dict().get('token')

    @property
    def me(self) -> User:
        return self.client.current_user.me()

    @property
    def catalog(self) -> Union[str, None]:
        """
        :return: default catalog name. If default catalog does not exist, return None
        """
        from davidkhala.databricks.workspace.catalog import Catalog

        c = Catalog(self.client)
        if c.get(c.default):
            return c.default
        else:
            return None

    @property
    def cloud(self):
        host = self.config.host

        if host.startswith('https://adb-') and host.endswith('.azuredatabricks.net'):
            # adb-662901427557763.3.azuredatabricks.net
            return 'azure'
        elif host.startswith('https://dbc-') and host.endswith('.cloud.databricks.com'):
            # dbc-8df7b30e-676a.cloud.databricks.com
            return "aws"
        elif host.endswith('.gcp.databricks.com'):
            # 1105010096141051.1.gcp.databricks.com
            return "gcp"
        return None

    @property
    def metastore(self):
        """
        :return: current metastore
        """
        return self.client.metastores.current()

    @property
    def dbutils(self) -> RemoteDbUtils:
        return self.client.dbutils

    @property
    def api_client(self):
        return APIClient(self.client)


class APIClient:
    api_version = '/api/2.0'
    client: ApiClient

    def __init__(self, client: WorkspaceClient):
        self.client = client.api_client

    def get(self, route, data=None):
        return self.client.do(method='GET', path=self.api_version + route, body=data)
