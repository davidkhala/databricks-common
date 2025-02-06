import os
from typing import Callable, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter

from davidkhala.databricks.connect import Session
from davidkhala.databricks.sink.stream import Internal, Write
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from davidkhala.databricks.workspace.table import Table
from davidkhala.databricks.workspace.volume import Volume


def to_table(df: DataFrame, table, w: Workspace, spark: SparkSession, timeout=10,
             *,
             on_start: Callable[[Write, DataStreamWriter], Any] = None,
             ):
    i = Internal(df, Session(spark).serverless)
    Table(w.client).delete(f"{w.catalog}.default.{table}")
    volume = Volume(w, table)
    volume.delete()
    i.onStart = on_start
    query = i.toTable(table, volume)

    query.awaitTermination(timeout)
    r: DataFrame = spark.sql('select * from ' + table)
    return r


def tearDown(spark: SparkSession, cluster: Cluster = None):
    if os.environ.get("ci") and cluster:
        cluster.stop()
    spark.stop()
