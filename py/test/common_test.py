import unittest

from py.common.local import CONFIG_PATH
from py.workspace import Workspace


class CommonTest(unittest.TestCase):

    config = Workspace.from_local().config

    def setUp(self):
        print(self.config)
        print(CONFIG_PATH)




if __name__ == '__main__':
    unittest.main()