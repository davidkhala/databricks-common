import os

from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.catalog import Schema, Catalog


def clear(schema, catalog=None):
    Schema(Workspace(), catalog).delete(schema)
    if catalog:
        Catalog(Workspace()).delete(catalog)

def is_databricks_notebook():
    """
    is current context a Databricks notebook
    :return:
    """
    return 'DATABRICKS_RUNTIME_VERSION' in os.environ