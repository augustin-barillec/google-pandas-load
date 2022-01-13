import pandas
from tests.utils import ids
from tests.utils import load
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


class WriteDispositionTest(BaseClassTest):

    def test_write_disposition_default_gs_to_bq(self):
        expected = pandas.DataFrame(data={'x': [1]})
        blob_name = ids.build_blob_name_2('a10')
        load.dataframe_to_gs(expected, blob_name)
        for _ in range(2):
            loaders.gpl21.load(
                source='gs',
                destination='bq',
                data_name='a10')
        computed = load.bq_to_dataframe('a10')
        self.assertTrue(expected.equals(computed))

    def test_write_truncate_query_to_bq(self):
        expected = pandas.DataFrame(data={'x': [1]})
        for _ in range(2):
            loaders.gpl21.load(
                source='query',
                destination='bq',
                query='select 1 as x',
                data_name='a10',
                write_disposition='WRITE_TRUNCATE')
        computed = load.bq_to_dataframe('a10')
        self.assertTrue(expected.equals(computed))

    def test_write_empty_local_to_bq(self):
        expected = pandas.DataFrame(data={'x': [1]})
        local_file_path = ids.build_local_file_path_1('a10')
        load.dataframe_to_local(expected, local_file_path)
        loaders.gpl01.load(
            source='local',
            destination='bq',
            data_name='a10',
            write_disposition='WRITE_EMPTY')
        computed = load.bq_to_dataframe('a10')
        self.assertTrue(expected.equals(computed))

    def test_write_append_dataframe_to_bq(self):
        expected = pandas.DataFrame(data={'x': [0, 1]})
        df00 = pandas.DataFrame(data={'x': [0]})
        df01 = pandas.DataFrame(data={'x': [1]})
        loaders.gpl00.load(
            source='dataframe',
            destination='bq',
            data_name='a10',
            dataframe=df00)
        loaders.gpl00.load(
            source='dataframe',
            destination='bq',
            data_name='a10',
            dataframe=df01,
            write_disposition='WRITE_APPEND')
        computed = load.bq_to_dataframe('a10')
        self.assertTrue(expected.equals(computed))
