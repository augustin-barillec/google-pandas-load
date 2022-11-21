import binascii
import pandas
from tests import utils


def is_gz_file(filepath):
    with open(filepath, 'rb') as f:
        return binascii.hexlify(f.read(2)) == b'1f8b'


class CompressTest(utils.base_class.BaseClassTest):
    def test_compress_query_to_bucket(self):
        gpl = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_subdir_path)
        gpl.load(
            source='query',
            destination='bucket',
            query='select 5',
            data_name='b100')
        blob_name = utils.ids.build_blob_name_2('b100-000000000000.csv.gz')
        local_file_path = utils.ids.build_local_file_path_1(
            'b100-000000000000.csv.gz')
        utils.load.bucket_to_local(blob_name, local_file_path)
        self.assertTrue(is_gz_file(local_file_path))

    def test_compress_dataframe_to_local(self):
        gpl = utils.loader.create_loader_quick_setup(
            project_id=None,
            dataset_name=None,
            bucket_name=None,
            local_dir_path=utils.constants.local_subdir_path)
        gpl.load(
            source='dataframe',
            destination='local',
            dataframe=pandas.DataFrame(data={'x': [1]}),
            data_name='b100')
        local_file_path = utils.ids.build_local_file_path_1('b100.csv.gz')
        self.assertTrue(is_gz_file(local_file_path))
