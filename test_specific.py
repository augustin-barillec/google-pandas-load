import unittest
from tests.test_compress import CompressTest

suite = unittest.TestSuite()
suite.addTest(CompressTest('test_compress_dataframe_to_local'))
unittest.TextTestRunner(verbosity=2).run(suite)
