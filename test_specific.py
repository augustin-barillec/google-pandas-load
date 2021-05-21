import unittest
from tests.test_logging import LoggingTest

suite = unittest.TestSuite()
suite.addTest(LoggingTest('test_default_loader_logger'))
unittest.TextTestRunner(verbosity=2).run(suite)
