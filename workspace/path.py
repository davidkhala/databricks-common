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

    notebookView = "notebooks_dimension"

    @property
    def notebook_gtv(self):
        """
        full name of notebook dimension GlobalTempView
        :return:
        """
        return 'global_temp.' + self.notebookView

    def index_notebooks(self, spark: SparkSession) -> bool:
        """
        :param spark:
        :type spark: pyspark.sql.SparkSession
        :return: True if found any notebooks, False otherwise
        """
        _notebooks = self.scan_notebooks()
        if len(_notebooks) == 0:
            return False
        notebook_dataframe = spark.createDataFrame(_notebooks, ["object_id", "path"])
        notebook_dataframe.createOrReplaceGlobalTempView(self.notebookView)
        return True

    def get_by(self, spark: SparkSession, *, notebook_id: str = None, path: str = None):
        if spark.catalog.tableExists(self.notebook_gtv):
            if path:
                _any = spark.sql(f"select object_id from {self.notebook_gtv} where path LIKE '%{path}%'").first()
                if _any:
                    return _any.object_id
            elif notebook_id:
                _any = spark.sql(f"select path from {self.notebook_gtv} where object_id = {notebook_id}").first()
                if _any:
                    return _any.path

            else:
                raise "Either notebook_id or path is required"

        return
