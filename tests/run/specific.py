import unittest
from tests.unit.test_cast import CastTest

suite = unittest.TestSuite()
suite.addTest(CastTest('test_bq_schema_inferred'))
unittest.TextTestRunner(verbosity=2).run(suite)
