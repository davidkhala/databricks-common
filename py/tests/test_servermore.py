import unittest

from databricks.sdk import WorkspaceClient

from davidkhala.databricks.workspace.server import Library


class LibraryTestCase(unittest.TestCase):
    def test_library_add(self):
        client = WorkspaceClient()
        library = Library(client)
        library.pollute()
        package_name = 'davidkhala-devops[new-relic]'
        library.add(package_name)
        library.uninstall_a(package_name)


if __name__ == '__main__':
    unittest.main()
