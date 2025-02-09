import os
import unittest
from datetime import datetime
from time import sleep
from typing import cast

from davidkhala.gcp.auth import OptionsInterface
from davidkhala.gcp.auth.service_account import from_service_account, ServiceAccount
from davidkhala.gcp.pubsub.pub import Pub
from davidkhala.gcp.pubsub.sub import Sub
from google.cloud.pubsub_v1.subscriber.message import Message
from pyspark.errors.exceptions.connect import AnalysisException

from davidkhala.databricks.gcp.pubsub import PubSub
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from tests.servermore import get
from tests.stream import to_table, tearDown


class PubSubTestCase(unittest.TestCase):
    controller: Cluster
    topic_id = 'databricks'
    subscription_id = 'spark'

    def setUp(self):
        private_key = os.environ.get('PRIVATE_KEY')
        info = ServiceAccount.Info(
            client_email=os.environ.get('CLIENT_EMAIL'),
            private_key=private_key,
            client_id=os.environ.get('CLIENT_ID'),
            private_key_id=os.environ.get('PRIVATE_KEY_ID')
        )

        self.auth = from_service_account(info)

        OptionsInterface.token.fget(self.auth)

        self.w = Workspace()
        self.spark, self.controller = get(self.w)

        self.pubsub = PubSub(None, self.spark).with_service_account(info)

        self.controller.start()
        print('PubSubTestCase setUp: completed')

    message: str
    to_be_ack: Message = None

    def write_to_table(self, timeout, finality):
        df = self.pubsub.read_stream(self.topic_id, self.subscription_id)

        def on_start(*args):
            pub = Pub(self.topic_id, self.auth)

            self.message = f"hello world at {datetime.now().timestamp()}"
            if finality:
                sub = Sub(self.subscription_id, self.topic_id, self.auth)
                msg_id = pub.publish(self.message)
                print('pubsub: published ' + msg_id)

                def on_event(message: Message, future):
                    print(message.data)
                    # Not to message.ack(): otherwise databricks cannot receive data
                    self.to_be_ack = message
                    future.cancel()

                sub.listen(on_event)
                sub.disconnect()
            else:
                pub.publish_async(self.message)

        table = 'pubsub'
        r = to_table(df, table, self.w, self.spark, timeout,
                     on_start=on_start
                     )
        poll_count = 1
        while r.count() == 0:
            sleep(1)
            print(f"poll...{poll_count}")
            poll_count += 1
            r = self.spark.sql('select * from ' + table)
        if self.to_be_ack:
            self.to_be_ack.ack()
        return r

    def test_read_stream(self):

        r = self.write_to_table(1, False)

        self.assertGreaterEqual(1, r.count())
        self.assertEqual(self.message, cast(bytearray, r.first()['payload']).decode('utf-8'))

    def test_read_batch(self):

        df = (self.spark.read.format("pubsub")
              .option("subscriptionId", self.subscription_id)
              .option("topicId", self.topic_id)
              .options(**self.pubsub.auth)
              .load()
              )

        with self.assertRaisesRegex(AnalysisException, "pubsub is not a valid Spark SQL Data Source."):
            df.printSchema()

    def tearDown(self):
        tearDown(self.spark, self.controller)


if __name__ == '__main__':
    unittest.main()
