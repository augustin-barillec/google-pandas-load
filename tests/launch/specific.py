import unittest
from tests.unit.test_load_parameters import LoadParametersTest
from tests.unit.test_display_log import DisplayLogTest
from tests.unit.test_data_delivery import DataDeliveryTest

suite = unittest.TestSuite()
# suite.addTests(unittest.TestLoader().loadTestsFromTestCase(DataDeliveryTest))
suite.addTest(DataDeliveryTest('dataframe_to_query'))
unittest.TextTestRunner(verbosity=2).run(suite)
