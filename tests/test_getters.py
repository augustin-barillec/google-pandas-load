import unittest
from tests import utils


class GettersTest(unittest.TestCase):
    def test_call_loader_getters(self):
        gpl00 = utils.loader.create_loader()
        gpl10 = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_dir_path)
        gpl20 = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_subdir_path)
        gpl01 = utils.loader.create_loader_quick_setup(
            project_id=None,
            dataset_name=None,
            bucket_name=None,
            local_dir_path=utils.constants.local_subdir_path)
        self.assertIsNotNone(gpl00.bq_client)
        self.assertIsNotNone(gpl00.gs_client)
        self.assertIsNotNone(gpl00.bucket)
        self.assertEqual(utils.constants.dataset_id, gpl00.dataset_id)
        self.assertEqual(utils.constants.dataset_name, gpl00.dataset_name)
        self.assertEqual(utils.constants.bucket_name, gpl00.bucket_name)
        self.assertIsNone(gpl00.bucket_dir_path)
        self.assertEqual(
            utils.constants.bucket_dir_path, gpl10.bucket_dir_path)
        self.assertEqual(
            utils.constants.bucket_subdir_path, gpl20.bucket_dir_path)
        self.assertEqual(utils.constants.local_dir_path, gpl00.local_dir_path)
        self.assertEqual(
            utils.constants.local_subdir_path, gpl01.local_dir_path)

    def test_call_loader_quick_setup_getters(self):
        gpl = utils.loader.create_loader_quick_setup(bucket_name=None)
        self.assertEqual(utils.constants.project_id, gpl.project_id)
