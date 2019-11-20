import binascii
from tests.context.loaders import *
from tests.utils import *


def is_gz_file(filepath):
    with open(filepath, 'rb') as test_f:
        return binascii.hexlify(test_f.read(2)) == b'1f8b'


class CompressTest(BaseClassTest):

    def test_compress_bq_to_gs(self):
        populate()

        gpl2.load(
            source='query',
            destination='local',
            data_name='b10',
            query='select 5')
        local_file_path = gpl1.list_local_file_paths('b10')[0]
        self.assertEqual(local_file_path,
                         os.path.join(local_dir_path, 'b10.csv.gz'))
        self.assertTrue(is_gz_file(local_file_path))

        gpl3.load(
            source='query',
            destination='local',
            data_name='a10',
            query='select 5')
        local_file_path = gpl1.list_local_file_paths('a10')[0]
        self.assertEqual(local_file_path,
                         os.path.join(local_dir_path, 'a10-000000000000.csv'))
        self.assertFalse(is_gz_file(local_file_path))

    def test_compress_dataframe_to_local(self):

        gpl2.load(
            source='dataframe',
            destination='local',
            data_name='e111',
            dataframe=pandas.DataFrame(data={'x': [1]}))
        local_file_path = gpl1.list_local_file_paths('e111')[0]
        self.assertEqual(local_file_path,
                         os.path.join(local_dir_path, 'e111.csv.gz'))
        self.assertTrue(is_gz_file(local_file_path))

        gpl4.load(
            source='dataframe',
            destination='local',
            data_name='e112',
            dataframe=pandas.DataFrame(data={'x': [1]}))
        local_file_path = gpl1.list_local_file_paths('e112')[0]
        self.assertEqual(local_file_path,
                         os.path.join(local_dir_path, 'e112.csv'))
        self.assertFalse(is_gz_file(local_file_path))
