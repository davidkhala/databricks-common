import requests
import json
from context import *


def get_table_lineage(table_name):
    # function to get lineage of unity catalog tables
    response = requests.get(
        API_URL + "/api/2.0/lineage-tracking/table-lineage",
        headers={"Authorization": "Bearer " + TOKEN},
        data=json.dumps({"table_name": table_name, "include_entity_lineage": True}),
    ).json()

    return response
