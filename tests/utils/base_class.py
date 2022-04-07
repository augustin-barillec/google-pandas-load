import unittest
from tests.utils import pandas_equal
from tests.utils.clean import clean
from tests.utils.create import create_local_subdir


class BaseClassTest(unittest.TestCase):

    @staticmethod
    def assert_pandas_equal(expected, computed):
        pandas_equal.assert_equal(expected, computed)

    def setUp(self):
        clean()
        create_local_subdir()

    def tearDown(self):
        clean()
