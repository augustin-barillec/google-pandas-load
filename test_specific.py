import unittest
from tests import test_error

suite = unittest.TestSuite()
suite.addTest(test_error.LoaderSetupErrorTest('test_raise_error_if_bq_client_none_dataset_id_not_none'))
unittest.TextTestRunner(verbosity=2).run(suite)
