import os

from workspace.query import Query
from syntax.fs import read
class TableLineage(Query):

    def all(self):
        current_dir = os.path.dirname(__file__)
        sql = read(os.path.join(current_dir, 'table.sql'))
        return self.run(sql)
