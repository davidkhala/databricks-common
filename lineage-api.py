
# MAGIC %sql 
# MAGIC -- assume Global Temp view exist
# MAGIC CREATE WIDGET TEXT notebooks_dimension_table_name DEFAULT "notebooks_dimension";
# MAGIC
# MAGIC select * from global_temp.${notebooks_dimension_table_name}

# COMMAND ----------

# function to get lineage of unity catalog tables
import requests
import json

dbutils.widgets.text("table_name", "azure-open-datasets.nyctlc.yellow", "")
API_URL = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .getOrElse(None)
)
TOKEN = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)


def get_table_lineage(table_name):
    
    response = requests.get(
        API_URL + '/api/2.0/lineage-tracking/table-lineage',
        headers={"Authorization": "Bearer " + TOKEN},
        data=json.dumps({'table_name':table_name, 'include_entity_lineage':True})
    ).json()

    return response



get_table_lineage(dbutils.widgets.get("table_name"))


# COMMAND ----------

def get_column_lineage(table_name):
    
    response = requests.get(
        API_URL + '/api/2.0/lineage-tracking/column-lineage',
        headers={"Authorization": "Bearer " + TOKEN},
        data=json.dumps({'table_name':table_name})
    ).json()

    return response



get_column_lineage(dbutils.widgets.get("table_name"))
