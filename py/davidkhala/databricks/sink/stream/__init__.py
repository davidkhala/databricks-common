from typing import Callable, Any

from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame
from pyspark.sql.connect.streaming.query import StreamingQuery
from pyspark.sql.streaming import DataStreamWriter

from davidkhala.databricks.workspace.volume import Volume


class Write:
    stream: DataStreamWriter
    serverless: bool

    def __init__(self, df: DataFrame, serverless=False):
        assert df.isStreaming
        self.stream = df.writeStream
        self.serverless = serverless

    def with_trigger(self, **kwargs):
        if self.serverless:
            self.stream = self.stream.trigger(availableNow=True)
        else:
            kwargs.setdefault(
                'processingTime',
                '0 seconds'
            )
            self.stream = self.stream.trigger(**kwargs)
        return self.stream


class Table(Write):
    onStart: Callable[[Write, DataStreamWriter], Any] = None

    def persist(self, table_name: str, volume: Volume = None, *, client: WorkspaceClient = None) -> StreamingQuery:
        if volume is None:
            from davidkhala.databricks.workspace import Workspace
            volume = Volume(Workspace(client), table_name)
        volume.create()

        writer: DataStreamWriter = self.stream.option("checkpointLocation", f"{volume.path}/checkpoint")
        if self.onStart:
            self.onStart(self, writer)
        return writer.toTable(table_name)

    def memory(self, queryName: str) -> StreamingQuery:
        writer: DataStreamWriter = self.stream.format("memory").queryName(queryName)
        if self.onStart:
            self.onStart(self, writer)
        return writer.start()
