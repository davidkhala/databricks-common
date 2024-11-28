import os

from databricks.sdk import WorkspaceClient
from syntax.fs import read

from workspace.warehouse import Warehouse

pwd = os.path.dirname(__file__)


# TODO table lineage


class LineageQuery:
    def __init__(self, query: Warehouse):
        self._baseSQL = read(os.path.join(pwd, self.__class__.__name__ + '.sql'))
        self._query = query

    def all(self):
        return self._query.run(self._baseSQL)


class Table(LineageQuery):
    # def join_notebook_dimension(self):
    #     spark = self._query
    #     # Load the DataFrames (replace with actual data loading methods)
    #     table_lineage_df = spark.table("system.access.table_lineage")
    #     notebooks_dimension_df = spark.table("schema.notebooks_dimension")
    #
    #     # Perform the join
    #     joined_df = table_lineage_df.join(
    #         notebooks_dimension_df,
    #         (table_lineage_df.entity_id == notebooks_dimension_df.object_id) &
    #         (table_lineage_df.entity_type == 'NOTEBOOK'),
    #         how='left'
    #     )
    #
    #     # Apply the CASE logic and select the required columns
    #     result_df = joined_df.select(
    #         when(col("entity_type") == 'NOTEBOOK', col("nd.path"))
    #         .otherwise(col("entity_id")).alias("notebook_path"),
    #         col("source_type"),
    #         col("source_table_full_name"),
    #         col("target_type"),
    #         col("target_table_full_name")
    #     ).filter(
    #         col("source_table_full_name").isNotNull() &
    #         col("target_table_full_name").isNotNull()
    #     )


class Column(LineageQuery):
    # TODO



