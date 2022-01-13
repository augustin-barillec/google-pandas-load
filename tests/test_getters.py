import unittest
from tests.utils.resources import project_id, \
    dataset_name, dataset_id, bucket_name, gs_dir_path, gs_subdir_path, \
    local_dir_path, local_subdir_path
from tests.utils import loaders


class GettersTest(unittest.TestCase):

    def test_call_loader_getters(self):
        self.assertIsNotNone(loaders.gpl10.bq_client)
        self.assertIsNotNone(loaders.gpl10.gs_client)
        self.assertIsNotNone(loaders.gpl10.bucket)
        self.assertEqual(dataset_id, loaders.gpl10.dataset_id)
        self.assertEqual(dataset_name, loaders.gpl10.dataset_name)
        self.assertEqual(bucket_name, loaders.gpl10.bucket_name)
        self.assertIsNone(loaders.gpl00.gs_dir_path)
        self.assertEqual(gs_dir_path, loaders.gpl10.gs_dir_path)
        self.assertEqual(gs_subdir_path, loaders.gpl20.gs_dir_path)
        self.assertEqual(local_dir_path, loaders.gpl10.local_dir_path)
        self.assertEqual(local_subdir_path, loaders.gpl11.local_dir_path)

    def test_call_loader_quick_setup_getters(self):
        self.assertEqual(project_id, loaders.gpl11.project_id)
