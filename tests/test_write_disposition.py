import pandas
from tests.utils import ids
from tests.utils import load
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


class WriteDispositionTest(BaseClassTest):

    def test_write_disposition_default_bucket_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [1]})
        blob_name = ids.build_blob_name_2('s10')
        load.dataframe_to_bucket(expected, blob_name)
        for _ in range(2):
            loaders.gpl21.load(
                source='bucket',
                destination='dataset',
                data_name='s10')
        computed = load.dataset_to_dataframe('s10')
        self.assert_pandas_equal(expected, computed)

    def test_write_truncate_query_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [1]})
        for _ in range(2):
            loaders.gpl21.load(
                source='query',
                destination='dataset',
                query='select 1 as x',
                data_name='s11',
                write_disposition='WRITE_TRUNCATE')
        computed = load.dataset_to_dataframe('s11')
        self.assert_pandas_equal(expected, computed)

    def test_write_empty_local_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [1]})
        local_file_path = ids.build_local_file_path_1('s12')
        load.dataframe_to_local(expected, local_file_path)
        loaders.gpl01.load(
            source='local',
            destination='dataset',
            data_name='s12',
            write_disposition='WRITE_EMPTY')
        computed = load.dataset_to_dataframe('s12')
        self.assert_pandas_equal(expected, computed)

    def test_write_append_dataframe_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [0, 1]})
        df00 = pandas.DataFrame(data={'x': [0]})
        df01 = pandas.DataFrame(data={'x': [1]})
        loaders.gpl00.load(
            source='dataframe',
            destination='dataset',
            dataframe=df00,
            data_name='s13')
        loaders.gpl00.load(
            source='dataframe',
            destination='dataset',
            dataframe=df01,
            data_name='s13',
            write_disposition='WRITE_APPEND')
        computed = load.dataset_to_dataframe('s13')
        self.assert_pandas_equal(expected, computed)
