import unittest
from tests.unit.test_load_parameters import LoadParametersTest
from tests.unit.test_display_log import DisplayLogTest

suite = unittest.TestSuite()
suite.addTests(unittest.TestLoader().loadTestsFromTestCase(DisplayLogTest))
# suite.addTest(CompressTest('test_compress_dataframe_to_local'))
unittest.TextTestRunner(verbosity=2).run(suite)
