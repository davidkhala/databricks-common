from typing import Callable, Any

from databricks.sdk import WorkspaceClient
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.streaming.query import StreamingQuery
from pyspark.sql.connect.streaming.readwriter import DataStreamWriter

from davidkhala.databricks.workspace.volume import Volume


class Write:
    def __init__(self, df: DataFrame, serverless=False):
        assert df.isStreaming
        self.stream: DataStreamWriter = df.writeStream
        self.serverless: bool = serverless

    @property
    def spark(self) -> SparkSession:
        return self.stream._session

    def with_trigger(self, **option):
        if self.serverless:
            self.stream = self.stream.trigger(availableNow=True)
        else:
            if not option:
                option = {
                    'processingTime': '0 seconds'
                }
            self.stream = self.stream.trigger(**option)
        return self.stream


class Table(Write):
    onStart: Callable[["Write", DataStreamWriter], Any] = None

    def persist(self, table_name: str, volume: Volume = None, *, client: WorkspaceClient = None) -> StreamingQuery:
        if volume is None:
            from davidkhala.databricks.workspace import Workspace
            volume = Volume(Workspace(client), table_name)
        volume.create()

        writer: DataStreamWriter = self.stream.option("checkpointLocation", f"{volume.path}")
        if self.onStart:
            self.onStart(self, writer)
        return writer.toTable(table_name)

    def memory(self, queryName: str) -> StreamingQuery:
        writer: DataStreamWriter = self.stream.format("memory").queryName(queryName)
        if self.onStart:
            self.onStart(self, writer)
        return writer.start()
