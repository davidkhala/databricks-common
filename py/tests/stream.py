import os
from time import sleep
from typing import Callable, Any

from pyspark.sql import DataFrame, SparkSession

from davidkhala.databricks.connect import Session
from davidkhala.databricks.sink.stream import Table as SinkTable
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from davidkhala.databricks.workspace.table import Table
from davidkhala.databricks.workspace.volume import Volume


def wait_data(spark, _sql, poll_count=1, interceptor: Callable[[DataFrame, int], Any] = None):
    r: DataFrame = spark.sql(_sql)

    if r.count() == 0:
        sleep(1)
        if interceptor:
            interceptor(r, poll_count)
        print(f"poll...{poll_count}")
        return wait_data(spark, _sql, poll_count + 1, interceptor)
    else:
        return r


def to_table(df: DataFrame, table, w: Workspace, spark: SparkSession):
    t = SinkTable(df, Session(spark).serverless)
    Table(w.client).delete(f"{w.catalog}.default.{table}")
    volume = Volume(w, table)
    volume.delete()
    query = t.persist(table, volume)

    return query, f"select * from {table}"


def to_memory(df: DataFrame, spark: SparkSession):
    t = SinkTable(df, Session(spark).serverless)
    mem_table = "streaming_memory_table"
    query = t.memory(mem_table)

    return query, f"select * from {mem_table}"


def tear_down(spark: SparkSession, cluster: Cluster = None):
    if os.environ.get("ci") and cluster:
        cluster.stop()
    spark.interruptAll()
    spark.stop()
