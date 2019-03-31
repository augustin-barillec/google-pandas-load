import unittest
from tests.unit.test_list_methods import ListMethodsTest
from tests.unit.test_exist_methods import ExistMethodsTest
from tests.unit.test_delete_methods import DeleteMethodsTest
from tests.unit.test_data_delivery import DataDeliveryTest

suite = unittest.TestSuite()
suite.addTest(DataDeliveryTest('query_to_dataframe'))
unittest.TextTestRunner(verbosity=2).run(suite)
