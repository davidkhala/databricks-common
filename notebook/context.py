# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0
# MAGIC

# COMMAND ----------

from databricks.sdk.runtime import dbutils, display

Context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
#
apiUrl = Context.apiUrl().get() # 'https://southeastasia.azuredatabricks.net'
apiToken = Context.apiToken().get()
workspaceId = Context.workspaceId().get() # '662901427557763'
dbr_version=spark.sql('select current_version().dbr_version').first()[0]

display({
    'apiUrl': apiUrl,
    'workspaceId': workspaceId,
    'dbr_version':dbr_version,
})