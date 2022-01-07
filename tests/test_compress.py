import os
import binascii
import pandas
from tests.resources import local_dir_path, local_subdir_path
from tests.base_class import BaseClassTest
from tests import loaders


def is_gz_file(filepath):
    with open(filepath, 'rb') as test_f:
        return binascii.hexlify(test_f.read(2)) == b'1f8b'


class CompressTest(BaseClassTest):

    def test_compress_bq_to_gs(self):
        expected = os.path.join(local_dir_path, 'b100-000000000000.csv.gz')
        loaders.gpl20.load(
            source='query',
            destination='local',
            data_name='b100',
            query='select 5')
        computed = loaders.gpl20.list_local_file_paths('b100')[0]
        self.assertEqual(expected, computed)
        self.assertTrue(is_gz_file(computed))

    def test_compress_dataframe_to_local(self):
        loaders.gpl01.load(
            source='dataframe',
            destination='local',
            data_name='b100',
            dataframe=pandas.DataFrame(data={'x': [1]}))
        expected = os.path.join(local_subdir_path, 'b100.csv.gz')
        computed = loaders.gpl01.list_local_file_paths('b100')[0]
        self.assertEqual(expected, computed)
        self.assertTrue(is_gz_file(computed))
