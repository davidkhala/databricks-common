from databricks.sdk import WorkspaceClient
from pyspark.sql.connect.session import SparkSession

from davidkhala.databricks.datasets import Loader as BaseLoader
from davidkhala.databricks.datasets.COVID import catalog
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

schema_path = 'covid-19-data'
schema = 'covid_19_data'


class Loader(BaseLoader):
    def __init__(self, w: WorkspaceClient, spark: SparkSession):
        super().__init__(w, spark, table_path_map,
                         schema=schema, schema_path= schema_path,
                         catalog=catalog)
