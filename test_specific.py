import unittest
from tests import test_error

suite = unittest.TestSuite()
suite.addTest(test_error.LoaderSetupErrorTest('test_raise_error_if_dataset_id_not_contain_exactly_one_dot'))
unittest.TextTestRunner(verbosity=2).run(suite)
