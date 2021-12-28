import unittest
from tests.test_list_methods import ListMethodsTest

suite = unittest.TestSuite()
suite.addTest(ListMethodsTest('test_list_blobs'))
unittest.TextTestRunner(verbosity=2).run(suite)
