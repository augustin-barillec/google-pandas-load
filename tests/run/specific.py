import unittest
from tests.unit.test_load_parameters import LoadParametersTest

suite = unittest.TestSuite()
suite.addTest(LoadParametersTest('test_wildcard'))
unittest.TextTestRunner(verbosity=2).run(suite)
