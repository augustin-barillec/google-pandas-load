import binascii
import pandas
from tests.utils import ids
from tests.utils import load
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


def is_gz_file(filepath):
    with open(filepath, 'rb') as f:
        return binascii.hexlify(f.read(2)) == b'1f8b'


class CompressTest(BaseClassTest):

    def test_compress_query_to_gs(self):
        loaders.gpl20.load(
            source='query',
            destination='gs',
            data_name='b100',
            query='select 5')
        blob_name = ids.build_blob_name_2('b100-000000000000.csv.gz')
        local_file_path = ids.build_local_file_path_1(
            'b100-000000000000.csv.gz')
        load.gs_to_local(blob_name, local_file_path)
        self.assertTrue(is_gz_file(local_file_path))

    def test_compress_dataframe_to_local(self):
        loaders.gpl01.load(
            source='dataframe',
            destination='local',
            data_name='b100',
            dataframe=pandas.DataFrame(data={'x': [1]}))
        local_file_path = ids.build_local_file_path_1('b100.csv.gz')
        self.assertTrue(is_gz_file(local_file_path))
