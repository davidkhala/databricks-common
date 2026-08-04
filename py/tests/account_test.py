import os
import unittest

from davidkhala.databricks.workspace import Workspace
from databricks.sdk import AccountClient


class AccountTest(unittest.TestCase):
    def setUp(self):
        w = Workspace.from_local()
        print(w.config)
        self.account_id = w.config.account_id
        self.azure_client_secret = os.environ.get("AZURE_CLIENT_SECRET")

    def test_tier(self):
        # account-level API requires Azure AD / OAuth credentials, not a workspace PAT.
        # pre-requisite: add your App registration(s) in Databricks account console [User management]/[Service principals] and then assign role to it

        account = AccountClient(
            host='https://accounts.azuredatabricks.net',
            account_id=self.account_id,
            azure_client_id='fa318cde-43db-40a5-a372-5159113bf7d8',
            azure_client_secret=self.azure_client_secret,
            azure_tenant_id='c2a38aca-e9c7-4647-8dcd-9185476159ae'
        )
        _list = list(account.workspaces.list())
        self.assertGreaterEqual(len(_list), 1)

        for workspace in _list:
            print(workspace.workspace_name, workspace.pricing_tier)
