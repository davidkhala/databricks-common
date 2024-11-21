import unittest

from syntax.format import JSONReadable
from syntax.fs import write

from workspace import Workspace
from workspace.query import Query
from workspace.table import Table


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.w = Workspace()

    def test_workspace(self):
        clusters = self.w.clusters()
        self.assertGreaterEqual(len(clusters), 0)


class QueryTest(unittest.TestCase):
    def setUp(self):
        warehouse = '/sql/1.0/warehouses/7969d92540da7f02'
        self.q = Query(Workspace().client, warehouse)

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

        write("table-lineage.json", JSONReadable(r))

class TableTest(unittest.TestCase):
    def setUp(self):
        self.t = Table(Workspace().client)

    def test_table_get(self):
        r = self.t.get("azure-open-datasets.nyctlc.yellow")
        write("azure-open-datasets.nyctlc.yellow.json", JSONReadable(r))
