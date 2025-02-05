import os
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from davidkhala.databricks.connect import Session
from davidkhala.databricks.sink.stream import Internal
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from davidkhala.databricks.workspace.table import Table
from davidkhala.databricks.workspace.volume import Volume


def to_table(df: DataFrame, table, w: Workspace, spark: SparkSession, timeout=10, onStart:Callable[[StreamingQuery], None]=None):
    i = Internal(df, Session(spark).serverless)
    Table(w.client).delete(f"{w.catalog}.default.{table}")
    volume = Volume(w, table)
    volume.delete()
    query = i.toTable(table, volume)
    if onStart:
        onStart(query)
    query.awaitTermination(timeout)
    r: DataFrame = spark.sql('select * from ' + table)
    return r


def tearDown(spark: SparkSession, cluster: Cluster=None):
    if os.environ.get("ci") and cluster:
        cluster.stop()
    spark.stop()
