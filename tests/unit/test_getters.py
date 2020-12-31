import unittest
from tests.context.loaders import gpl2, gpl5
from tests.context.resources import project_id, \
    dataset_name, bucket_name, local_dir_path


class CallGetters(unittest.TestCase):

    def test_call_loader_getters(self):
        self.assertIsNotNone(gpl2.bq_client)
        self.assertIsNotNone(gpl2.dataset_ref)
        self.assertIsNotNone(gpl2.bucket)
        self.assertIsNotNone(gpl2.bq_client)
        self.assertEqual(gpl2.dataset_name, dataset_name)
        self.assertEqual(gpl2.bucket_name, bucket_name)
        self.assertEqual(gpl2.gs_dir_path, 'dir/subdir')
        self.assertEqual(gpl2.local_dir_path, local_dir_path)

    def test_call_loader_quick_setup_getters(self):
        self.assertEqual(gpl5.project_id, project_id)
        self.assertIsNotNone(gpl5.gs_client)
