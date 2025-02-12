import os
import unittest
from datetime import datetime
from typing import cast

from davidkhala.gcp.auth import OptionsInterface
from davidkhala.gcp.auth.service_account import from_service_account, ServiceAccount
from davidkhala.gcp.pubsub.pub import Pub
from davidkhala.gcp.pubsub.sub import Sub
from pyspark.errors.exceptions.connect import AnalysisException

from davidkhala.databricks.gcp.pubsub import PubSub
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from tests.servermore import get
from tests.stream import to_table, tearDown, wait_data, to_memory


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
        self.pub = Pub(self.topic_id, self.auth)
        self.sub = Sub(self.subscription_id, self.topic_id, self.auth)
        OptionsInterface.token.fget(self.auth)

        self.w = Workspace()
        self.spark, self.controller = get(self.w)

        self.pubsub = PubSub(None, self.spark).with_service_account(info)

    message: str

    def on_start(self, *args):
        self.message = f"hello world at {datetime.now().timestamp()}"
        self.pub.publish(self.message)

    def test_sink_table(self):
        self.controller.start()
        df = self.pubsub.read_stream(self.topic_id, self.subscription_id)

        table = 'pubsub'
        _, _sql = to_table(df, table, self.w, self.spark, on_start=self.on_start)

        r = wait_data(self.spark, _sql)

        self.assertGreaterEqual(r.count(), 1)
        self.assertEqual(self.message, cast(bytearray, r.first()['payload']).decode('utf-8'))

    def test_service_principle_permission(self):
        # TODO, this take longer than expect
        import uuid
        sub_id = f"topic_{uuid.uuid4().hex}"
        self.sub.subscription = sub_id
        self.sub.create()
        self.sub.delete()

    def test_sink_memory(self):
        self.controller.start()
        df = self.pubsub.read_stream(self.topic_id, self.subscription_id)
        # FIXME without subscription_id cannot work

        _, _sql = to_memory(df, self.spark, on_start=self.on_start)

        r = wait_data(self.spark, _sql)
        self.assertGreaterEqual(r.count(), 1)
        self.assertEqual(self.message, cast(bytearray, r.first()['payload']).decode('utf-8'))

    def test_read_batch(self):
        self.controller.start()
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
        self.sub.purge()


if __name__ == '__main__':
    unittest.main()
