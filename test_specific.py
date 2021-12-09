import unittest
from tests.test_raise_error import LoaderSetupRaiseErrorTest

suite = unittest.TestSuite()
suite.addTest(LoaderSetupRaiseErrorTest('test_raise_error_if_gs_dir_path_is_empty_string'))
unittest.TextTestRunner(verbosity=2).run(suite)
