from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.catalog import Schema, Catalog


def clear(schema, catalog=None):
    Schema(Workspace(),schema, catalog).delete()
    if catalog:
        Catalog(Workspace()).delete(catalog)

