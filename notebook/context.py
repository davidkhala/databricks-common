from databricks.sdk.runtime import dbutils, display

Context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
#
apiUrl = Context.apiUrl().get() # 'https://southeastasia.azuredatabricks.net'
apiToken = Context.apiToken().get()
workspaceId = Context.workspaceId().get() # '662901427557763'

display({
    'apiUrl': apiUrl,
    'workspaceId': workspaceId,
})