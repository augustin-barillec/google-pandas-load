import unittest
from tests.test_data_delivery import DataDeliveryTest

suite = unittest.TestSuite()
suite.addTest(DataDeliveryTest('test_no_skip_blank_lines'))
unittest.TextTestRunner(verbosity=2).run(suite)
