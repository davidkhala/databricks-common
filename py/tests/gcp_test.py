import os
import unittest
import warnings
from datetime import datetime
from typing import cast

from davidkhala.gcp.auth import OptionsInterface
from davidkhala.gcp.auth.service_account import from_service_account, ServiceAccount
from davidkhala.gcp.pubsub.pub import Pub
from davidkhala.gcp.pubsub.sub import Sub
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.sql.connect.streaming.readwriter import DataStreamReader

from davidkhala.databricks.gcp.pubsub import PubSub
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from tests.servermore import get
from tests.stream import to_table, tear_down, wait_data, mem_table


class PubSubTestCase(unittest.TestCase):
    controller: Cluster
    topic_id = 'databricks'
    subscription_id = 'spark'

    def setUp(self):
        private_key = os.environ.get('PRIVATE_KEY')
        info = ServiceAccount.Info(
            client_email=os.environ.get(
                'CLIENT_EMAIL') or 'data-integration@gcp-data-davidkhala.iam.gserviceaccount.com',
            private_key=private_key,
            client_id=os.environ.get('CLIENT_ID') or '105034720006001204003',
            private_key_id=os.environ.get('PRIVATE_KEY_ID') or '48aaad07d7a0285896adb47ebd81ca7907c42d35'
        )

        self.auth = from_service_account(info)
        self.pub = Pub(self.topic_id, self.auth)
        self.sub = Sub(self.subscription_id, self.topic_id, self.auth)
        OptionsInterface.token.fget(self.auth)

        self.w = Workspace()
        self.spark, self.controller = get(self.w)

        self.pubsub = PubSub(None, self.spark).with_service_account(info)
        self.controller.start()

    message: str | None = None

    def publish(self):
        self.message = f"hello world at {datetime.now()}"
        self.pub.publish(self.message)
        warnings.warn(f"self.pub.publish({self.message})")

    def test_publish(self):
        self.publish()

    def test_sink_table(self):
        df = self.pubsub.read_stream(self.topic_id, self.subscription_id).read_start()

        table = 'pubsub'
        query, _sql = to_table(df, table, self.w, self.spark)

        r = wait_data(self.spark, _sql, 1, lambda *_: self.on_ready(query))

        self.assertGreaterEqual(r.count(), 1)
        self.assertEqual(self.message, cast(bytearray, r.first()['payload']).decode('utf-8'))

    def on_ready(self, query):
        s = query.status
        #  {'message': 'Processing new data', 'isDataAvailable': True, 'isTriggerActive': True}
        if (
                s['message'] == 'Waiting for data to arrive'
                and s['isDataAvailable'] == False
                and s['isTriggerActive'] == False
                and not self.message
        ):
            self.publish()

    def test_sink_memory(self):
        self.sink_memory(True, False)
        self.sink_memory(False, False)
        self.sink_memory(False, True)
        # self.sink_memory(True, True) # Poll until 60 seconds timeout. No data available

    def sink_memory(self, random_sub, with_trigger):

        if with_trigger:
            # TODO delete the sub
            self.publish()
        self.pubsub.read_stream(self.topic_id, self.subscription_id if not random_sub else None)
        df = self.pubsub.read_start()
        from davidkhala.databricks.sink.stream import Table as SinkTable
        from davidkhala.databricks.connect import Session
        t = SinkTable(df, Session(self.spark).serverless)
        if with_trigger:
            t.stream.trigger(availableNow=True)
        query = t.memory(mem_table)

        _sql = f"select * from {mem_table}"

        def on_ready(*_):
            if not with_trigger:
                self.on_ready(query)
            else:
                print(query.status)

        r = wait_data(self.spark, _sql, 1, on_ready)
        self.assertEqual(1, r.count())
        self.assertEqual(self.message, cast(bytearray, r.first()['payload']).decode('utf-8'))
        # cleanup
        query.stop()
        self.spark.sql(f"DROP TABLE {mem_table}")
        self.message = None
        self.sub.purge()

    def test_cleanup(self):
        self.spark.sql(f"DROP TABLE IF EXISTS {mem_table}")
        self.sub.purge()

    def test_read_batch(self):

        source:DataStreamReader = (self.spark.read.format("pubsub")
                  .option("subscriptionId", self.subscription_id)
                  .option("topicId", self.topic_id)
                  .options(**self.pubsub.auth)
                  )
        print(source)
        print(source._options)
        df = source.load()
        with self.assertRaisesRegex(AnalysisException, "pubsub is not a valid Spark SQL Data Source."):
            df.printSchema()

    def tearDown(self):
        tear_down(self.spark, self.controller)
        self.sub.purge()


if __name__ == '__main__':
    unittest.main()
