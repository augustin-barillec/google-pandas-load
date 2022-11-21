import pandas
from tests import utils


class WriteDispositionTest(utils.base_class.BaseClassTest):
    def test_write_disposition_default_bucket_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [1]})
        blob_name = utils.ids.build_blob_name_2('s10')
        utils.load.dataframe_to_bucket(expected, blob_name)
        gpl = utils.loader.create_loader_quick_setup(
            bucket_dir_path=utils.constants.bucket_subdir_path,
            local_dir_path=None)
        for _ in range(2):
            gpl.load(
                source='bucket',
                destination='dataset',
                data_name='s10')
        computed = utils.load.dataset_to_dataframe('s10')
        self.assert_pandas_equal(expected, computed)

    def test_write_truncate_query_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [1]})
        gpl = utils.loader.create_loader_quick_setup(
            bucket_name=None,
            local_dir_path=None)
        for _ in range(2):
            gpl.load(
                source='query',
                destination='dataset',
                query='select 1 as x',
                data_name='s11',
                write_disposition='WRITE_TRUNCATE')
        computed = utils.load.dataset_to_dataframe('s11')
        self.assert_pandas_equal(expected, computed)

    def test_write_empty_local_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [1]})
        local_file_path = utils.ids.build_local_file_path_1('s12')
        utils.load.dataframe_to_local(expected, local_file_path)
        gpl = utils.loader.create_loader(
            local_dir_path=utils.constants.local_subdir_path)
        gpl.load(
            source='local',
            destination='dataset',
            data_name='s12',
            write_disposition='WRITE_EMPTY')
        computed = utils.load.dataset_to_dataframe('s12')
        self.assert_pandas_equal(expected, computed)

    def test_write_append_dataframe_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [0, 1]})
        df00 = pandas.DataFrame(data={'x': [0]})
        df01 = pandas.DataFrame(data={'x': [1]})
        gpl = utils.loader.create_loader(chunk_size=2**18, timeout=5)
        gpl.load(
            source='dataframe',
            destination='dataset',
            dataframe=df00,
            data_name='s13')
        gpl.load(
            source='dataframe',
            destination='dataset',
            dataframe=df01,
            data_name='s13',
            write_disposition='WRITE_APPEND')
        computed = utils.load.dataset_to_dataframe('s13')
        self.assert_pandas_equal(expected, computed)
