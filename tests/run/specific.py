import unittest
from tests.unit.test_cast import CastTest

suite = unittest.TestSuite()
suite.addTest(CastTest('test_bq_schema_inferred_from_csv'))
unittest.TextTestRunner(verbosity=2).run(suite)
