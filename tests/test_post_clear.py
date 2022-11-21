import pandas
from tests import utils


class PostClearTest(utils.base_class.BaseClassTest):
    def test_post_clear_query_to_dataframe(self):
        utils.populate.populate()
        blob_name = utils.ids.build_blob_name_0('a10')
        local_file_path = utils.ids.build_local_file_path_1('a10')
        self.assertTrue(utils.exist.table_exists('a10'))
        self.assertTrue(utils.exist.blob_exists(blob_name))
        self.assertTrue(utils.exist.local_file_exists(local_file_path))
        gpl = utils.loader.create_loader(
            local_dir_path=utils.constants.local_subdir_path)
        gpl.load(
            source='query',
            destination='dataframe',
            query='select 3',
            data_name='a10')
        self.assertFalse(utils.exist.table_exists('a10'))
        self.assertFalse(utils.exist.blob_exists(blob_name))
        self.assertFalse(utils.exist.local_file_exists(local_file_path))

    def test_post_clear_dataframe_to_dataset(self):
        utils.populate.populate()
        blob_name = utils.ids.build_blob_name_2('a10')
        local_file_path = utils.ids.build_local_file_path_0('a10')
        self.assertTrue(utils.exist.blob_exists(blob_name))
        self.assertTrue(utils.exist.local_file_exists(local_file_path))
        gpl = utils.loader.create_loader_quick_setup(
            bucket_dir_path=utils.constants.bucket_subdir_path)
        gpl.load(
            source='dataframe',
            destination='dataset',
            dataframe=pandas.DataFrame(data={'x': [1]}),
            data_name='a10')
        self.assertFalse(utils.exist.blob_exists(blob_name))
        self.assertFalse(utils.exist.local_file_exists(local_file_path))
