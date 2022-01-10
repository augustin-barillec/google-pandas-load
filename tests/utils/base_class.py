import unittest
from tests.utils.clean import clean
from tests.utils.create import create_local_subdir


class BaseClassTest(unittest.TestCase):
    def setUp(self):
        clean()
        create_local_subdir()

    def tearDown(self):
        clean()
