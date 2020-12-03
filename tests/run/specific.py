import unittest
from tests.unit.test_cast import CastTest

suite = unittest.TestSuite()
suite.addTest(CastTest('test_parse_dates'))
unittest.TextTestRunner(verbosity=2).run(suite)
