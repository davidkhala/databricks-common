# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0
# MAGIC

# COMMAND ----------

# MAGIC %sh python --version

# COMMAND ----------
# MAGIC %sh cat ~/.databrickscfg # No such file or directory
# COMMAND ----------
from databricks.sdk.runtime import dbutils, display, spark

Context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
#
apiUrl = Context.apiUrl().get()  # 'https://southeastasia.azuredatabricks.net'
apiToken = Context.apiToken().get()
workspaceId = Context.workspaceId().get()  # '662901427557763'
dbr_version = spark.sql('select current_version().dbr_version').first()[0]

display({
    'apiUrl': apiUrl,
    'workspaceId': workspaceId,
    'dbr_version': dbr_version,
})

# COMMAND ----------
from pyspark.sql import SparkSession

inline_spark = SparkSession.builder.getOrCreate()
print(inline_spark.catalog.currentCatalog() + '.' + inline_spark.catalog.currentDatabase())
inline_spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
