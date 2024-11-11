import requests
import json
from context import *


def get_path(path="/"):
    # function to retrieve objects within specified path within workspace
    response = requests.get(
        apiUrl + "/api/2.1/workspace/list",
        headers={"Authorization": "Bearer " + apiToken},
        data=json.dumps({"path": path}),
    ).json()

    return response



def scan_notebooks(path="/"):
    # Get Notebook Paths
    result = []
    response = get_path(path)
    if "objects" in response:
        for object_item in response["objects"]:
            if object_item["object_type"] == "NOTEBOOK":
                result.append([object_item["object_id"], object_item["path"]])
            elif object_item["object_type"] == "DIRECTORY":
                result = result + scan_notebooks(object_item["path"])
    return result


def index_notebooks(spark, GlobalTempView="notebooks_dimension"):
    notebook_dataFrame = spark.createDataFrame(scan_notebooks(), ["object_id", "path"])
    notebook_dataFrame.createOrReplaceGlobalTempView(GlobalTempView)
