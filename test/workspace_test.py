import unittest

from syntax.fs import write_json

from workspace import Workspace, path
from workspace.warehouse import Warehouse
from workspace.table import Table


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.w = Workspace()

    def test_client(self):
        print(self.w.config_token)

    def test_SDK(self):
        s = path.SDK.from_workspace(self.w)
        self.assertEqual(s.get_by(notebook_id='918032188629039'), '/Shared/context')
        self.assertEqual(s.get_by(path='context'), '918032188629039')

    def test_clusters(self):
        clusters = self.w.clusters()
        self.assertGreaterEqual(len(clusters), 0)


class WarehouseTest(unittest.TestCase):
    def setUp(self):
        warehouse = '/sql/1.0/warehouses/284d94956aa8f5c0'
        self.q = Warehouse(Workspace().client, warehouse)

    def test_query(self):
        r = self.q.run(
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
        self.t = Table(Workspace().client)

    def test_table_get(self):
        r = self.t.get("azure-open-datasets.nyctlc.yellow")
        write_json(r, "azure-open-datasets.nyctlc.yellow")
