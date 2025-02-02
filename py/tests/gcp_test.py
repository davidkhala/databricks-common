import os
import unittest

from davidkhala.gcp.auth import OptionsInterface
from davidkhala.gcp.auth.options import from_service_account

from davidkhala.databricks.gcp.pubsub import AuthOptions, PubSub
from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.server import Cluster
from tests.servermore import get


class PubSubTestCase(unittest.TestCase):
    auth = AuthOptions(
        clientId=os.environ.get('CLIENT_ID'),
        privateKey=os.environ.get('PRIVATE_KEY'),
        clientEmail=os.environ.get('CLIENT_EMAIL'),
        privateKeyId=os.environ.get('PRIVATE_KEY_ID'),
        projectId=os.environ.get('PROJECT_ID'),
    )
    controller: Cluster

    def setUp(self):

        _ = from_service_account(
            client_email=self.auth.get('clientEmail'),
            private_key=self.auth.get('privateKey'),
            project_id=self.auth.get('projectId'),
        )
        OptionsInterface.token.fget(_)

        w = Workspace()
        spark, self.controller = get(w)

        self.pubsub = PubSub(self.auth, spark)
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
