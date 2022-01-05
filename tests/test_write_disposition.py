import pandas
from google.cloud.exceptions import Conflict
from tests.base_class import BaseClassTest
from tests.resources import dataset_id, bq_client
from tests import loaders


class WriteDispositionTest(BaseClassTest):

    def test_write_disposition_default_gs_to_bq(self):
        df0 = pandas.DataFrame(data={'x': [1]})
        loaders.gpl21.load(
            source='dataframe',
            destination='gs',
            data_name='a10',
            dataframe=df0)
        for _ in range(2):
            loaders.gpl21.load(
                source='gs',
                destination='bq',
                data_name='a10')
        table_id = f'{dataset_id}.a10'
        query = f'select * from {table_id}'
        df1 = bq_client.query(query).to_dataframe()
        self.assertTrue(df0.equals(df1))

    def test_write_truncate_query_to_bq(self):
        df0 = pandas.DataFrame(data={'x': [1]})
        for _ in range(2):
            loaders.gpl21.load(
                source='query',
                destination='bq',
                query='select 1 as x',
                data_name='a10',
                write_disposition='WRITE_TRUNCATE')
        table_id = f'{dataset_id}.a10'
        query = f'select * from {table_id}'
        df1 = bq_client.query(query).to_dataframe()
        self.assertTrue(df0.equals(df1))

    def test_write_empty_local_to_bq(self):
        df0 = pandas.DataFrame(data={'x': [1]})
        loaders.gpl01.load(
            source='dataframe',
            destination='local',
            data_name='a10',
            dataframe=df0)
        loaders.gpl01.load(
            source='local',
            destination='bq',
            data_name='a10',
            write_disposition='WRITE_EMPTY')
        with self.assertRaises(Conflict) as cm:
            loaders.gpl01.load(
                source='local',
                destination='bq',
                data_name='a10',
                write_disposition='WRITE_EMPTY')
        self.assertEqual(
            str(cm.exception),
            '409 Already Exists: Table dmp-y-tests:test_gpl.a10')

    def test_write_append_dataframe_to_bq(self):
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
        table_id = f'{dataset_id}.a10'
        query = f'select * from {table_id} order by x'
        df1 = bq_client.query(query).to_dataframe()
        expected = pandas.concat([df00, df01]).reset_index(drop=True)
        self.assertTrue(expected.equals(df1))
