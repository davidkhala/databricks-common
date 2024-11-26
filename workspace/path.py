from pyspark.sql.connect.session import SparkSession

from workspace import APIClient


class WorkspacePath:
    def __init__(self, api_client: APIClient):
        self.api_client = api_client

    def ls(self, path="/"):
        """
        function to retrieve objects within specified path
        :param path:
        :return:
        """
        return self.api_client.get('/workspace/list', {'path': path})

    def scan_notebooks(self, path="/") -> list:
        """
        Get Notebook Paths
        :param path:
        :return:
        """
        result = []
        response = self.ls(path)
        if "objects" in response:
            for object_item in response["objects"]:
                if object_item["object_type"] == "NOTEBOOK":
                    result.append([object_item["object_id"], object_item["path"]])
                elif object_item["object_type"] == "DIRECTORY":
                    result = result + self.scan_notebooks(object_item["path"])
        return result

    defaultNotebookView = "notebooks_dimension"

    def index_notebooks(self, spark: SparkSession, GlobalTempView=defaultNotebookView) -> bool:
        """
        :param spark:
        :type spark: pyspark.sql.SparkSession
        :param GlobalTempView:
        :return: True if found any notebooks, False otherwise
        """
        _notebooks = self.scan_notebooks()
        if len(_notebooks) == 0:
            return False
        notebook_dataframe = spark.createDataFrame(_notebooks, ["object_id", "path"])
        notebook_dataframe.createOrReplaceGlobalTempView(GlobalTempView)
        return True

    @staticmethod
    def get_by(spark: SparkSession, notebook_id: str, GlobalTempView=defaultNotebookView):
        _full_name = 'global_temp.' + GlobalTempView
        if spark.catalog.tableExists(_full_name):
            _df = spark.sql(f"select path from {_full_name} where object_id = {notebook_id}")
            return _df
        return
