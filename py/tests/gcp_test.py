import os
import unittest

from davidkhala.gcp.auth import OptionsInterface
from davidkhala.gcp.auth.service_account import from_service_account, Info

from davidkhala.databricks.gcp.pubsub import PubSub
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from tests.servermore import get
from tests.stream import to_table, tearDown


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

        self.w = Workspace()
        self.spark, self.controller = get(self.w)

        self.pubsub = PubSub(None, self.spark).with_service_account(info)

        self.controller.start()
        print('setup completed')

    def test_read_stream(self):
        topic_id = 'databricks'
        subscription_id = 'community'
        df = self.pubsub.read_stream(topic_id, subscription_id)
        df.printSchema()
        # TODO WIP the show does not work
        # self.pubsub.show(df, 30)

        r = to_table(df, 'pubsub', self.w, self.spark, 30)
        r.show()  # TODO This works, next step is automate pubsub sending in parallel

    def test_read_batch(self):
        # TODO
        pass

    def tearDown(self):
        tearDown(self.spark, self.controller)


if __name__ == '__main__':
    unittest.main()
