import requests
import json
from notebook.context import *


def get_table_lineage(table_name):
    # function to get lineage of unity catalog tables
    response = requests.get(
        apiUrl + "/api/2.1/lineage-tracking/table-lineage",
        headers={"Authorization": "Bearer " + apiToken},
        data=json.dumps({"table_name": table_name, "include_entity_lineage": True}),
    ).json()

    return response
