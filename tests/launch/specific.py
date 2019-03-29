import unittest
from tests.unit.test_list_methods import ListMethodsTest
from tests.unit.test_exist_methods import ExistMethodsTest
from tests.unit.test_delete_methods import DeleteMethodsTest

suite = unittest.TestSuite()
suite.addTest(DeleteMethodsTest('test_delete_in_local'))
unittest.TextTestRunner(verbosity=2).run(suite)
