import unittest

from davidkhala.syntax.fs import write_json
from pyspark.sql.connect.session import SparkSession

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.table import Table

w = Workspace.from_local()
from davidkhala.databricks.lineage.rest import API as RESTAPI
from notebook.source.azure_open_datasets.nyctlc import NycTLC
from notebook.source.azure_open_datasets.context import catalog

class LineageTest(unittest.TestCase):
    t: Table
    api: RESTAPI
    spark: SparkSession

    @classmethod
    def setUpClass(cls):
        cls.api = RESTAPI(w.api_client)
        cls.t = Table(w.client)
        cls.spark, _ = DatabricksConnect.get()


    def test_API_lineage(self):
        instance = NycTLC(LineageTest.spark)
        instance.load()
        instance.copy_to_current()
        table_name = f"{catalog}.nyctlc.yellow"
        table_lineage = self.api.get_table(table_name)

        write_json(table_lineage, table_name + '.lineage')
        # column lineage
        columns = self.t.column_names(table_name)
        for column in columns:
            c_l = self.api.get_column(table_name, column)
            print(c_l)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
