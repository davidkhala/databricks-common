import os


def is_databricks_notebook():
    """
    is current context a Databricks notebook
    :return:
    """
    return 'DATABRICKS_RUNTIME_VERSION' in os.environ

def is_pat(token:str):
    return token.startswith("dapi") and token.endswith("-2")