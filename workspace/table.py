from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo


class Table:
    def __init__(self, client: WorkspaceClient):
        self.client = client

    def get(self, full_name: str):
        return Table.pretty(self.client.tables.get(full_name))

    def list(self, catalog_name: str, schema_name: str):
        return self.client.tables.list(catalog_name, schema_name)

    @staticmethod
    def pretty(table: TableInfo):
        d = table.as_dict()
        return {
            'catalog_name': d['catalog_name'],
            'columns': d['columns'],
            'comment': d['comment'],
            "data_source_format": d['data_source_format'],
            'name': d['name'],
            'schema_name': d['schema_name'],
            'table_id': d['table_id'],
        }
