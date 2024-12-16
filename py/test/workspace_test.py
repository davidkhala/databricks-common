import unittest

from davidkhala.syntax.fs import write_json

from davidkhala.databricks.workspace import Workspace, path
from davidkhala.databricks.workspace.catalog import Catalog, Schema
from davidkhala.databricks.workspace.table import Table
from davidkhala.databricks.workspace.warehouse import Warehouse

w = Workspace.from_local()


class WorkspaceTest(unittest.TestCase):

    def setUp(self):
        print(w.config_token)

    def test_SDK(self):
        s = path.SDK.from_workspace(w)
        self.assertEqual(s.get_by(notebook_id='918032188629039'), '/Shared/context')
        self.assertEqual(s.get_by(path='context'), '918032188629039')

    def test_clusters(self):
        clusters = w.clusters()
        self.assertGreaterEqual(len(clusters), 0)


class WarehouseTest(unittest.TestCase):
    def setUp(self):
        warehouse = '/sql/1.0/warehouses/f74f8ec14f4e81fa'
        self.w = Warehouse(w.client, warehouse)

    def test_active(self):
        self.w.activate()

    def test_stop(self):
        self.w.stop()

    def test_query(self):
        r = self.w.run(
            """
            select entity_type,
                   entity_id,
                   source_type,
                   source_table_full_name,
                   target_type,
                   target_table_full_name
            from system.access.table_lineage
            where source_table_full_name is not null
              and target_table_full_name is not null        
            """)
        write_json(r, 'table-lineage')


class TableTest(unittest.TestCase):
    def setUp(self):
        self.t = Table(w.client)

    def test_table_get(self):
        r = self.t.get("azure-open-datasets.nyctlc.yellow")
        write_json(r, "azure-open-datasets.nyctlc.yellow")


class CatalogTest(unittest.TestCase):
    def setUp(self):
        self.c = Catalog(w)
        self.s = Schema(w)

    def test_get(self):
        _ = self.c.get('not_exists')
        print(_)

    def test_create(self):
        self.c.create('test')

    def test_delete(self):
        self.c.delete('test')

    def test_schema_create(self):
        self.s.create('test')

    def test_schema_delete(self):
        self.s.delete('test')
