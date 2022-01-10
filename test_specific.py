import unittest
from tests import test_cast

suite = unittest.TestSuite()
suite.addTest(test_cast.CastTest('test_dtype_query_to_dataframe'))
unittest.TextTestRunner(verbosity=2).run(suite)
