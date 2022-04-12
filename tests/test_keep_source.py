from tests.utils import constants
from tests.utils.populate import populate_dataset, populate_bucket, \
    populate_local
from tests.utils import ids
from tests.utils import exist
from tests.utils.loader import create_loader
from tests.utils.base_class import BaseClassTest


class KeepSourceTest(BaseClassTest):

    def test_keep_source_in_dataset(self):
        populate_dataset()
        gpl = create_loader(
            bucket_dir_path=constants.bucket_subdir_path,
            local_dir_path=constants.local_subdir_path)
        gpl.load(
            source='dataset',
            destination='local',
            data_name='a7')
        self.assertTrue(exist.table_exists('a7'))

    def test_keep_source_in_bucket(self):
        populate_bucket()
        gpl = create_loader(bucket_dir_path=constants.bucket_subdir_path)
        gpl.load(
            source='bucket',
            destination='dataframe',
            data_name='a10')
        blob_name = ids.build_blob_name_2('a10')
        self.assertTrue(exist.blob_exists(blob_name))

    def test_keep_source_in_local(self):
        populate_local()
        gpl = create_loader(bucket_dir_path=constants.bucket_dir_path)
        gpl.load(
            source='local',
            destination='bucket',
            data_name='a10')
        local_file_path = ids.build_local_file_path_0('a10')
        self.assertTrue(exist.local_file_exists(local_file_path))
