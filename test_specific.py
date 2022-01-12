import unittest
from tests import test_data_delivery

suite = unittest.TestSuite()
suite.addTest(test_data_delivery.DataDeliveryTest('test_dataframe_to_gs'))
unittest.TextTestRunner(verbosity=2).run(suite)
