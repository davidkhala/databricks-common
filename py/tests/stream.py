from time import sleep
from typing import Callable, Any

from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.streaming.query import StreamingQuery

from davidkhala.databricks.connect import Session
from davidkhala.databricks.sink.stream import Table as SinkTable
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.table import Table
from davidkhala.databricks.workspace.volume import Volume


def wait_data(spark, _sql, poll_count=1, interceptor: Callable[[DataFrame, int], Any] = None):
    r: DataFrame = spark.sql(_sql)

    if r.count() == 0:
        sleep(1)
        if interceptor:
            signal = interceptor(r, poll_count)
            if signal: return
        print(f"poll...{poll_count}")
        return wait_data(spark, _sql, poll_count + 1, interceptor)
    else:
        return r


def clean(table, w: Workspace):
    Table(w.client).delete(f"{w.catalog}.default.{table}")
    volume = Volume(w, table)
    volume.delete()
    return volume


def to_table(df: DataFrame, table, w: Workspace, spark: SparkSession) -> (StreamingQuery, str):
    volume = Volume(w, table)
    t = SinkTable(df, Session(spark).serverless)
    t.with_trigger()
    query = t.persist(table, volume)

    return query, f"select * from {table}"


mem_table = "streaming_memory_table"
