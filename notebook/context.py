from databricks.sdk.runtime import dbutils, display, spark

Context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
#
apiUrl = Context.apiUrl().get()  # 'https://southeastasia.azuredatabricks.net'
apiToken = Context.apiToken().get()
workspaceId = Context.workspaceId().get()  # '662901427557763'
dbr_version = spark.sql('select current_version().dbr_version').first()[0] # used to inspect serverless compute runtime version

display({
    'apiUrl': apiUrl,
    'workspaceId': workspaceId,
    'dbr_version': dbr_version,
})
