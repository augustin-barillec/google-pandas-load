import unittest
from tests.unit.test_list_methods import ListMethodsTest

suite = unittest.TestSuite()
# suite.addTest(ListMethodsTest('test_list_blob_uris_when_gs_dir_path_is_None'))
# suite.addTest(ListMethodsTest('test_list_blob_uris_when_gs_dir_path_is_not_None'))
suite.addTest(ListMethodsTest('test_list_local_file_paths'))
unittest.TextTestRunner(verbosity=2).run(suite)
