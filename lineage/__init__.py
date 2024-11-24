import os

from databricks.sdk import WorkspaceClient
from syntax.fs import read

from workspace.query import Query

pwd = os.path.dirname(__file__)


class LineageQuery(Query):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.baseSQL = read(os.path.join(pwd, self.__class__.__name__ + '.sql'))
        self._sql = self.baseSQL

    def run(self):
        return super().run(self._sql)

    def with_source(self, full_name: str) -> 'LineageQuery':
        """
        :param full_name: source table full name
        :return:
        """
        self._sql += f" and source_table_full_name='{full_name}'"
        return self

    def with_target(self, full_name: str) -> 'LineageQuery':
        """
        :param full_name: target table full name
        :return:
        """
        self._sql += f" and target_table_full_name='{full_name}'"
        return self


class Table(LineageQuery):
    def __init__(self, client: WorkspaceClient, http_path: str):
        super().__init__(client, http_path)


class Column(LineageQuery):
    def __init__(self, client: WorkspaceClient, http_path: str):
        super().__init__(client, http_path)
