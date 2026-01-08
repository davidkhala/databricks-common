from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.catalog import Catalog, Schema
from davidkhala.databricks.workspace.table import Table

table_path_map = {
    "colleges": "colleges/colleges.csv",
    "excess_deaths": "excess-deaths/deaths.csv",
    "live_us_counties": "live/us-counties.csv",
    "live_us_states": "live/us-states.csv",
    "live_us": "live/us.csv",
    "mask_use": "mask-use/mask-use-by-county.csv",
    "us_counties_recent": "us-counties-recent.csv",
    "us_counties": "us-counties.csv",
    "us_states": "us-states.csv",
    "us": "us.csv",
}

catalog = 'COVID'
schema = 'covid-19-data'
class Loader:
    w = Workspace()
    c = Catalog(w.client)
    @classmethod
    def start(cls):
        cls.c.create(catalog)

        s = Schema(cls.w.client,schema, catalog )
        s.create()

        # load table if not exist, you need to use spark dataframe

