import unittest

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import PythonPyPiLibrary

from davidkhala.databricks.workspace.server import Cluster, Library


class LibraryTestCase(unittest.TestCase):
    def test_library_add(self):
        client = WorkspaceClient()
        Cluster(client).as_one().pollute()
        library = Library(client)
        package_name = 'davidkhala-devops[new-relic]'
        library.add(package_name)
        library.uninstall_a(package_name)


if __name__ == '__main__':
    unittest.main()
