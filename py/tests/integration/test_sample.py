import unittest

from davidkhala.databricks.connect import DatabricksConnect
from davidkhala.databricks.workspace import Workspace

w = Workspace.from_local()
spark, _ = DatabricksConnect.get()


class COVID(unittest.TestCase):
    def test_covid_19_data(self):
        from davidkhala.databricks.datasets.COVID.covid_19_data import Loader

        l = Loader(w.client, spark)
        l.start()
    def test_cord_19(self):
        self.skipTest("WIP")
        from davidkhala.databricks.datasets.COVID.CORD_19 import Loader
        l = Loader(w.client, spark)
        l.delete()
        l.start()


class DataGov(unittest.TestCase):
    def test_farmers_markets(self):
        from davidkhala.databricks.datasets.data_gov.farmers_markets_geographic import Loader
        l = Loader(w.client, spark)
        l.start()


if __name__ == '__main__':
    unittest.main()
