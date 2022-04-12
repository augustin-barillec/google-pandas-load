import pandas
from tests.utils import constants
from tests.utils.populate import populate
from tests.utils import ids
from tests.utils import exist
from tests.utils.loader import create_loader
from tests.utils.base_class import BaseClassTest


class PostClearTest(BaseClassTest):

    def test_post_clear_query_to_dataframe(self):
        populate()
        blob_name = ids.build_blob_name_0('a10')
        local_file_path = ids.build_local_file_path_1('a10')
        self.assertTrue(exist.table_exists('a10'))
        self.assertTrue(exist.blob_exists(blob_name))
        self.assertTrue(exist.local_file_exists(local_file_path))
        gpl = create_loader(local_dir_path=constants.local_subdir_path)
        gpl.load(
            source='query',
            destination='dataframe',
            query='select 3',
            data_name='a10')
        self.assertFalse(exist.table_exists('a10'))
        self.assertFalse(exist.blob_exists(blob_name))
        self.assertFalse(exist.local_file_exists(local_file_path))

    def test_post_clear_dataframe_to_dataset(self):
        populate()
        blob_name = ids.build_blob_name_2('a10')
        local_file_path = ids.build_local_file_path_0('a10')
        self.assertTrue(exist.blob_exists(blob_name))
        self.assertTrue(exist.local_file_exists(local_file_path))
        gpl = create_loader(bucket_dir_path=constants.bucket_subdir_path)
        gpl.load(
            source='dataframe',
            destination='dataset',
            dataframe=pandas.DataFrame(data={'x': [1]}),
            data_name='a10')
        self.assertFalse(exist.blob_exists(blob_name))
        self.assertFalse(exist.local_file_exists(local_file_path))
