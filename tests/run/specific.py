import unittest
from tests.unit.test_raise_error import LoadConfigRaiseErrorTest

suite = unittest.TestSuite()
suite.addTest(LoadConfigRaiseErrorTest('test_raise_error_if_infer_bq_schema_from_no_columns_dataframe'))
unittest.TextTestRunner(verbosity=2).run(suite)
