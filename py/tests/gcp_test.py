import os
import unittest

from davidkhala.gcp.auth import OptionsInterface
from davidkhala.gcp.auth.service_account import from_service_account, Info

from davidkhala.databricks.gcp.pubsub import PubSub
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from tests.servermore import get


class PubSubTestCase(unittest.TestCase):
    controller: Cluster

    def setUp(self):
        private_key = os.environ.get('PRIVATE_KEY')

        info = Info(
            client_email=os.environ.get('CLIENT_EMAIL'),
            private_key=private_key,
            client_id=os.environ.get('CLIENT_ID'),
            private_key_id=os.environ.get('PRIVATE_KEY_ID')
        )
        _ = from_service_account(info)

        OptionsInterface.token.fget(_)

        w = Workspace()
        spark, self.controller = get(w)

        self.pubsub = PubSub(None, spark)
        self.pubsub.with_service_account(info)
        self.controller.start()

    def test_read_stream(self):
        topic_id = 'databricks'
        subscription_id = 'community'
        df = self.pubsub.read_stream(topic_id, subscription_id)
        df.printSchema()
        # TODO WIP the show does not work
        self.pubsub.show(df, 10)

    def test_read_batch(self):
        # TODO
        pass

    def tearDown(self):
        self.pubsub.disconnect()
        # self.controller.stop()
        pass


if __name__ == '__main__':
    unittest.main()
