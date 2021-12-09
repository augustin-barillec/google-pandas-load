import unittest
from tests.test_raise_error import LoadRaiseErrorTest

suite = unittest.TestSuite()
suite.addTest(LoadRaiseErrorTest('test_raise_error_if_a_blob_name_contains_a_late_slash'))
unittest.TextTestRunner(verbosity=2).run(suite)
