from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.catalog import Schema, Catalog


def clear(schema, catalog=None):
    w = Workspace()
    Schema(w.client,schema, catalog).delete()
    if catalog:
        Catalog(w.client).delete(catalog)

