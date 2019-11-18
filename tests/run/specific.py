import unittest
from tests.unit.test_data_delivery import DataDeliveryTest

suite = unittest.TestSuite()
suite.addTest(DataDeliveryTest('test_dataframe_to_query'))
unittest.TextTestRunner(verbosity=2).run(suite)
