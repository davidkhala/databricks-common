from platform import python_version

python_version = python_version()

# Databricks context
from databricks.sdk.runtime import dbutils, spark

## workspace context
Context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
apiUrl = Context.apiUrl().get()
apiToken = Context.apiToken().get()
workspaceId = Context.workspaceId().get()
## spark context
dbr_version = spark.sql('select current_version().dbr_version').first()[0]
catalog = spark.catalog.currentCatalog()
schema = spark.catalog.currentDatabase()
clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
