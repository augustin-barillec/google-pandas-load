from tests import utils


class ExistTest(utils.base_class.BaseClassTest):
    def test_exist_in_dataset(self):
        gpl = utils.loader.create_loader_quick_setup(
            local_dir_path=utils.constants.local_subdir_path)
        self.assertFalse(gpl.exist_in_dataset('a8'))
        utils.populate.populate_dataset()
        self.assertTrue(gpl.exist_in_dataset('a8'))

    def test_exist_in_bucket(self):
        gpl01 = utils.loader.create_loader(
            local_dir_path=utils.constants.local_subdir_path)
        gpl11 = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_dir_path,
            local_dir_path=utils.constants.local_subdir_path)
        gpl21 = utils.loader.create_loader_quick_setup(
            dataset_name=None,
            bucket_dir_path=utils.constants.bucket_subdir_path,
            local_dir_path=utils.constants.local_subdir_path)
        self.assertFalse(gpl01.exist_in_bucket('a1'))
        self.assertFalse(gpl11.exist_in_bucket('a10'))
        self.assertFalse(gpl21.exist_in_bucket('a'))
        utils.populate.populate_bucket()
        self.assertTrue(gpl01.exist_in_bucket('a1'))
        self.assertTrue(gpl11.exist_in_bucket('a10'))
        self.assertTrue(gpl21.exist_in_bucket('a'))

    def test_exist_in_local(self):
        gpl00 = utils.loader.create_loader()
        gpl01 = utils.loader.create_loader(
            bq_client=None,
            dataset_id=None,
            gs_client=None,
            bucket_name=None,
            bucket_dir_path='bucket_dir_path',
            local_dir_path=utils.constants.local_subdir_path)
        self.assertFalse(gpl00.exist_in_local('a'))
        self.assertFalse(gpl01.exist_in_local('a9'))
        utils.populate.populate_local()
        self.assertTrue(gpl00.exist_in_local('a'))
        self.assertTrue(gpl01.exist_in_local('a9'))
