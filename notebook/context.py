from databricks.sdk.runtime import dbutils

Context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
#
apiUrl = Context.apiUrl().get()
apiToken = Context.apiToken().get()
workspaceId = Context.workspaceId().get()
