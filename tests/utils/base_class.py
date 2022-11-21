import unittest
from tests import utils


class BaseClassTest(unittest.TestCase):
    @staticmethod
    def assert_pandas_equal(expected, computed):
        utils.pandas_equal.assert_equal(expected, computed)

    def setUp(self):
        utils.clean.clean()
        utils.create.create_local_subdir()

    def tearDown(self):
        utils.clean.clean()
