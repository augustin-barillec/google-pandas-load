from tests.utils.populate import populate_dataset, populate_bucket, \
    populate_local
from tests.utils import ids
from tests.utils import exist
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


class KeepSourceTest(BaseClassTest):

    def test_keep_source_in_dataset(self):
        populate_dataset()
        loaders.gpl21.load(
            source='dataset',
            destination='local',
            data_name='a7')
        self.assertTrue(exist.table_exists('a7'))

    def test_keep_source_in_bucket(self):
        populate_bucket()
        loaders.gpl20.load(
            source='bucket',
            destination='dataframe',
            data_name='a10')
        blob_name = ids.build_blob_name_2('a10')
        self.assertTrue(exist.blob_exists(blob_name))

    def test_keep_source_in_local(self):
        populate_local()
        loaders.gpl10.load(
            source='local',
            destination='bucket',
            data_name='a10')
        local_file_path = ids.build_local_file_path_0('a10')
        self.assertTrue(exist.local_file_exists(local_file_path))
