from typing import Iterator

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App


class SparkApp:
    def __init__(self, client: WorkspaceClient):
        self.client = client

    def ls(self) -> Iterator[App]:
        return self.apps.list()

    @property
    def apps(self):
        return self.client.apps

    def names(self) -> Iterator[str]:
        return (app.name for app in self.ls())

    def purge(self):
        for name in self.names():
            self.apps.stop_and_wait(name)
            self.apps.delete(name)
