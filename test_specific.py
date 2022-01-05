import unittest
from tests.test_write_disposition import WriteDispositionTest

suite = unittest.TestSuite()
suite.addTest(WriteDispositionTest('test_write_append_dataframe_to_bq'))
unittest.TextTestRunner(verbosity=2).run(suite)
