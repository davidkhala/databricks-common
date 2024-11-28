import unittest

from syntax.fs import write_json

from lineage import Table as TableLineage, Column as ColumnLineage
from lineage.rest import API
from spark import DatabricksConnect
from workspace import Workspace
from workspace.path import WorkspacePath
from workspace.table import Table

w = Workspace()


class TestRest(unittest.TestCase):
    def setUp(self):
        self.api = API(w.api_client)
        self.t = Table(w.client)

    def test_API_lineage(self):
        table_name = 'azure-open-datasets.nyctlc.yellow'
        table_lineage = self.api.get_table(table_name)

        write_json(table_lineage, table_name + '.table')
        # column lineage
        columns = self.t.columns(table_name)
        for column in columns:
            c_l = self.api.get_column(table_name, column)
            print(c_l)


class TestLineage(unittest.TestCase):
    def setUp(self):
        warehouse = '/sql/1.0/warehouses/7969d92540da7f02'
        self.t = TableLineage(w.client, warehouse)
        self.c = ColumnLineage(w.client, warehouse)

    def test_table_lineage(self):
        l = self.t.run()
        write_json(l, "all-table-lineage")

    def test_column_lineage(self):
        c = self.c.run()
        write_json(c, "all-column-lineage")


class TestQuery(unittest.TestCase):
    def test_run(self):
        w.spark.sql('select 1')
        # GTV created by notebook cluster can be access by another session
        _sql = 'select * from global_temp.notebooks_dimension'
        df = w.spark.sql(_sql)
        df.show()


        warehouse_connect = DatabricksConnect(serverless=True, host=w.config.host, token=w.config.token)
        # GTV created by notebook cluster cannot be access by SQL warehouse
        warehouse_connect.run(_sql)

    def tearDown(self):
        w.disconnect()


class TestE2E(unittest.TestCase):
    spark = w.spark
    p = WorkspacePath(w.api_client)

    def setUp(self):
        self.p.index_notebooks(self.spark)

    def test_start(self):
        r = WorkspacePath.get_by(self.p, self.spark, notebook_id='918032188629039')
        print(r)

    def tearDown(self):
        w.connection.disconnect()


if __name__ == '__main__':
    unittest.main()
