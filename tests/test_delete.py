from tests import utils


class DeleteTest(utils.base_class.BaseClassTest):
    def test_delete_in_dataset(self):
        utils.populate.populate_dataset()
        self.assertTrue(utils.exist.table_exists('a11'))
        gpl = utils.loader.create_loader_quick_setup(
            bucket_name=None,
            bucket_dir_path=None,
            local_dir_path=None)
        gpl.delete_in_dataset('a11')
        self.assertFalse(utils.exist.table_exists('a11'))

    def test_delete_in_bucket(self):
        utils.populate.populate_bucket()

        blob_name = utils.ids.build_blob_name_0('a7')
        self.assertTrue(utils.exist.blob_exists(blob_name))
        gpl = utils.loader.create_loader(
            bq_client=None,
            dataset_id=None,
            local_dir_path=None)
        gpl.delete_in_bucket('a7')
        self.assertFalse(utils.exist.blob_exists(blob_name))

        blob_name = utils.ids.build_blob_name_1('a10')
        self.assertTrue(utils.exist.blob_exists(blob_name))
        gpl = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_dir_path)
        gpl.delete_in_bucket('a1')
        self.assertFalse(utils.exist.blob_exists(blob_name))

        blob_name = utils.ids.build_blob_name_2('a13')
        self.assertTrue(utils.exist.blob_exists(blob_name))
        gpl = utils.loader.create_loader_quick_setup(
            dataset_name=None,
            bucket_dir_path=utils.constants.bucket_subdir_path,
            local_dir_path=None)
        gpl.delete_in_bucket('a')
        self.assertFalse(utils.exist.blob_exists(blob_name))

    def test_delete_in_local(self):
        utils.populate.populate_local()

        local_file_path = utils.ids.build_local_file_path_0('a11')
        self.assertTrue(utils.exist.local_file_exists(local_file_path))
        gpl = utils.loader.create_loader(
            bq_client=None,
            dataset_id=None,
            gs_client=None,
            bucket_name=None)
        gpl.delete_in_local('a11')
        self.assertFalse(utils.exist.local_file_exists(local_file_path))

        local_file_path = utils.ids.build_local_file_path_1('a12')
        self.assertTrue(utils.exist.local_file_exists(local_file_path))
        gpl = utils.loader.create_loader_quick_setup(
            project_id=None,
            dataset_name=None,
            bucket_name=None,
            local_dir_path=utils.constants.local_subdir_path)
        gpl.delete_in_local('a1')
        self.assertFalse(utils.exist.local_file_exists(local_file_path))
