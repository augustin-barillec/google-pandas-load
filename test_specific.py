import unittest
from tests.test_logging import LoggingTest

suite = unittest.TestSuite()
suite.addTest(LoggingTest('test_query_to_gs'))
unittest.TextTestRunner(verbosity=2).run(suite)
