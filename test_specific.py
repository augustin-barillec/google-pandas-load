import unittest
from tests import test_raise_error

suite = unittest.TestSuite()
suite.addTest(test_raise_error.LoadRaiseErrorTest('test_wait_for_jobs_runtime_error'))
unittest.TextTestRunner(verbosity=2).run(suite)
