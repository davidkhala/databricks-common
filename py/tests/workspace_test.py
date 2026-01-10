import os
import unittest

from davidkhala.utils.syntax.fs import write_json

from davidkhala.databricks.workspace import Workspace, path
from davidkhala.databricks.workspace.catalog import Catalog, Schema
from davidkhala.databricks.workspace.job import Job
from davidkhala.databricks.workspace.server import Cluster
from davidkhala.databricks.workspace.warehouse import Warehouse

w = Workspace.from_local()


class WorkspaceTest(unittest.TestCase):

    def test_default_catalog(self):
        self.assertEqual(w.catalog, os.environ.get("CATALOG") or "az_databricks")
    def test_notebook(self):
        s = path.SDK.from_workspace(w)
        local_notebook_path = os.path.join(os.path.dirname(__file__), "../notebook/context.ipynb")

        s.upload_notebook(local_notebook_path, '/Shared/context')
        notebook_id = s.get_by(path='context')
        print('notebook (context) id', notebook_id)
        self.assertEqual(s.get_by(notebook_id=notebook_id), '/Shared/context')

    def test_clusters(self):
        c = Cluster(w.client)
        self.assertGreaterEqual(len(list(c.clusters())), 0)
    def test_tier(self):
        from databricks.sdk import AccountClient
        print(w.config)
        account = AccountClient()
        for workspace in account.workspaces.list():
            print(workspace.workspace_name, workspace.pricing_tier)
    def test_account(self):

        self.skipTest(
            'databricks error: User is not a member of this account.')
        # TODO login troubleshoot
        from databricks.sdk import AccountClient
        account = AccountClient()
        for workspace in account.workspaces.list():
            print(workspace.workspace_name, workspace.pricing_tier)
from davidkhala.databricks.workspace.table import Table


class TableTest(unittest.TestCase):
    t = Table(w.client)

    def test_table_get(self):
        table_name = "samples.nyctaxi.trips"
        r = self.t.get(table_name)
        write_json(r, table_name)


from davidkhala.databricks.workspace.volume import Volume


class VolumeTest(unittest.TestCase):
    v = Volume(w, 'new')
    fs = v.fs

    def test_volume_get(self):
        print(self.v.get())

    def test_volume_create(self):
        self.v.create()

    def test_volume_delete(self):
        self.v.delete()


from davidkhala.databricks.workspace.volume import Volume


class VolumeFSTest(unittest.TestCase):
    v = Volume(w, 'volume')
    fs = v.fs

    @classmethod
    def setUpClass(cls):
        cls.v.create()

    def test_fs_upload(self):
        self.fs.upload('self/pyproject.toml')
        print(self.fs.read('pyproject.toml'))

    @classmethod
    def tearDownClass(cls):
        cls.v.delete()


class WarehouseTest(unittest.TestCase):
    w = Warehouse(w.client)

    @classmethod
    def setUpClass(cls):
        cls.w.get_one()

    def test_list(self):
        for warehouse in self.w.ls():
            print(warehouse.id)

    def test_active(self):
        self.w.start()

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
        write_json(Warehouse.pretty(r), 'table-lineage')


class CatalogTest(unittest.TestCase):
    c = Catalog(w.client)
    s = Schema(w.client, 'test')

    def test_get(self):
        self.assertIsNone(self.c.get('not_exists'))
        print(self.c.get().storage_root) # print default

    def test_create(self):
        self.c.create('test')

    def test_delete(self):
        self.c.delete('test')

    def test_schema_create(self):
        self.s.create()

    def test_schema_delete(self):
        self.s.delete()


class JobTest(unittest.TestCase):
    job = Job(w.client)

    def test_jobs(self):
        for job in self.job.ls():
            print(job.job_id)
