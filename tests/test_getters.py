import unittest
from tests.utils.constants import project_id, \
    dataset_name, dataset_id, bucket_name, bucket_dir_path, \
    bucket_subdir_path, local_dir_path, local_subdir_path
from tests.utils.loader import create_loader, create_loader_quick_setup


class GettersTest(unittest.TestCase):

    def test_call_loader_getters(self):
        gpl00 = create_loader()
        gpl10 = create_loader(bucket_dir_path=bucket_dir_path)
        gpl20 = create_loader(bucket_dir_path=bucket_subdir_path)
        gpl01 = create_loader(local_dir_path=local_subdir_path)
        self.assertIsNotNone(gpl00.bq_client)
        self.assertIsNotNone(gpl00.gs_client)
        self.assertIsNotNone(gpl00.bucket)
        self.assertEqual(dataset_id, gpl00.dataset_id)
        self.assertEqual(dataset_name, gpl00.dataset_name)
        self.assertEqual(bucket_name, gpl00.bucket_name)
        self.assertIsNone(gpl00.bucket_dir_path)
        self.assertEqual(bucket_dir_path, gpl10.bucket_dir_path)
        self.assertEqual(bucket_subdir_path, gpl20.bucket_dir_path)
        self.assertEqual(local_dir_path, gpl00.local_dir_path)
        self.assertEqual(local_subdir_path, gpl01.local_dir_path)

    def test_call_loader_quick_setup_getters(self):
        gpl = create_loader_quick_setup()
        self.assertEqual(project_id, gpl.project_id)
