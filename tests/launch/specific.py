import unittest
from tests.unit.test_load_parameters import LoadParametersTest
from tests.unit.test_data_delivery import DataDeliveryTest

suite = unittest.TestSuite()
# suite.addTests(unittest.TestLoader().loadTestsFromTestCase(DataDeliveryTest))
suite.addTest(DataDeliveryTest('query_to_dataframe'))
unittest.TextTestRunner(verbosity=2).run(suite)
