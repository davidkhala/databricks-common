import unittest

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace

w = Workspace.from_local()
spark, _ = DatabricksConnect.get()

# TODO
targets = [
            'colleges', 'excess-deaths', 'live', 'mask-use'
        ]

class COVID(unittest.TestCase):
    def test_covid_19_data(self):
        from davidkhala.databricks.datasets.COVID.covid_19_data import Loader

        l = Loader(w.client, spark)
        l.start()


if __name__ == '__main__':
    unittest.main()
