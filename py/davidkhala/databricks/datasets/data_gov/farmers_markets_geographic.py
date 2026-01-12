from databricks.sdk import WorkspaceClient
from pyspark.sql.connect.session import SparkSession
from davidkhala.databricks.datasets import Loader as BaseLoader
table_path_map = {
    "data": "data-001/market_data.csv",
}
schema = 'farmers_markets_geographic'
catalog = 'data_gov'
catalog_path = 'data.gov'
schema_path = 'farmers_markets_geographic_data'

class Loader(BaseLoader):
    def __init__(self, w: WorkspaceClient, spark: SparkSession):
        super().__init__(w, spark, table_path_map,
                         schema=schema, schema_path=schema_path,
                         catalog=catalog, catalog_path=catalog_path)