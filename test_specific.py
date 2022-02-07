import unittest
from tests import test_write_disposition

suite = unittest.TestSuite()
suite.addTest(test_write_disposition.WriteDispositionTest('test_write_empty_local_to_bq'))
unittest.TextTestRunner(verbosity=2).run(suite)
