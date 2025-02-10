from typing import TypedDict

from davidkhala.gcp.auth.service_account import ServiceAccount
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
    spark: Session | SparkSession
    source: DataStreamReader
    sink: DataStreamWriter

    def __init__(self, auth: AuthOptions | None, spark: SparkSession):
        self.auth = auth
        s = Session(spark)
        assert s.is_servermore()  # Not support in serverless compute
        self.spark = s

    def with_service_account(self, info: ServiceAccount.Info):
        self.auth = AuthOptions(
            clientId=info.get('client_id'),
            clientEmail=info.get('client_email'),
            privateKey=info.get('private_key'),
            privateKeyId=info.get('private_key_id'),
            projectId=info.get('project_id'),
        )
        return self

    def read_stream(self, topic_id, subscription_id=None):
        _sub_id = subscription_id
        if subscription_id is None:
            _sub_id = self.spark.appName

        stream_reader: DataStreamReader = (
            self.spark.readStream.format("pubsub")
            .option('deleteSubscriptionOnStreamStop', subscription_id is None)
            .option("subscriptionId", _sub_id)
            .option("topicId", topic_id)
            .options(**self.auth)
        )

        # Set up the streaming DataFrame to listen to the Pub/Sub topic
        pubsub_df = stream_reader.load()
        assert pubsub_df.isStreaming == True
        return pubsub_df

    def disconnect(self):
        self.spark.disconnect()
