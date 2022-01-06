import unittest
from tests import test_data_delivery

suite = unittest.TestSuite()
suite.addTest(test_data_delivery.DataDeliveryTest('test_query_to_bq'))
unittest.TextTestRunner(verbosity=2).run(suite)
