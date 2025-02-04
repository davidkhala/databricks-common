from typing import TypedDict

from davidkhala.gcp.auth.service_account import Info
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter, StreamingQuery

from davidkhala.databricks.connect import Session


class AuthOptions(TypedDict):
    clientId: str
    clientEmail: str
    privateKey: str
    privateKeyId: str
    projectId: str


class PubSub:
    auth: AuthOptions
    spark: SparkSession
    source: DataStreamReader
    sink: DataStreamWriter

    def __init__(self, auth: AuthOptions | None, spark: SparkSession):
        self.auth = auth
        assert Session(spark).is_servermore()  # Not support in serverless compute
        self.spark = spark

    def with_service_account(self, info: Info):
        self.auth = AuthOptions(
            clientId=info.get('client_id'),
            clientEmail=info.get('client_email'),
            privateKey=info.get('private_key'),
            privateKeyId=info.get('private_key_id'),
            projectId=info.get('project_id'),
        )
        return self

    def read_stream(self, topic_id, subscription_id):
        stream_reader: DataStreamReader = (
            self.spark.readStream.format("pubsub")
            .option("subscriptionId", subscription_id)
            .option("topicId", topic_id)
            .options(**self.auth)
        )

        # Set up the streaming DataFrame to listen to the Pub/Sub topic
        pubsub_df = stream_reader.load()
        assert pubsub_df.isStreaming == True
        return pubsub_df

    def show(self, pubsub_df: DataFrame, timeout=0):
        assert pubsub_df.isStreaming == True
        # TODO
        print('stream start')
        query: StreamingQuery = (
            pubsub_df.writeStream
            .foreachBatch(lambda df, batch_id: df.show())
            .start()
        )
        query.awaitTermination(timeout)

    def disconnect(self):
        self.spark.stop()
