from tests import utils


class KeepSourceTest(utils.base_class.BaseClassTest):
    def test_keep_source_in_dataset(self):
        utils.populate.populate_dataset()
        gpl = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_subdir_path,
            local_dir_path=utils.constants.local_subdir_path)
        gpl.load(
            source='dataset',
            destination='local',
            data_name='a7')
        self.assertTrue(utils.exist.table_exists('a7'))

    def test_keep_source_in_bucket(self):
        utils.populate.populate_bucket()
        gpl = utils.loader.create_loader_quick_setup(
            bucket_dir_path=utils.constants.bucket_subdir_path)
        gpl.load(
            source='bucket',
            destination='dataframe',
            data_name='a10')
        blob_name = utils.ids.build_blob_name_2('a10')
        self.assertTrue(utils.exist.blob_exists(blob_name))

    def test_keep_source_in_local(self):
        utils.populate.populate_local()
        gpl = utils.loader.create_loader_quick_setup(
            dataset_name=None,
            bucket_dir_path=utils.constants.bucket_dir_path)
        gpl.load(
            source='local',
            destination='bucket',
            data_name='a10')
        local_file_path = utils.ids.build_local_file_path_0('a10')
        self.assertTrue(utils.exist.local_file_exists(local_file_path))
