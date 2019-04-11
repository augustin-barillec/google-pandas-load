from google_pandas_load import LoadConfig
from tests.context.loaders import *
from tests.utils import *


class DataDeliveryTest(BaseClassTest):

    def test_query_to_bq(self):
        l0 = [2, 3]
        populate_dataset()
        gpl3.load(
            source='query',
            destination='bq',
            data_name='a0',
            query='select 3 as x union all select 2 as x')
        table_ref = dataset_ref.table(table_id='a0')
        table = bq_client.get_table(table_ref)
        df1 = bq_client.list_rows(table=table).to_dataframe()
        l1 = sorted(list(df1.x))
        self.assertEqual(l0, l1)

    def test_bq_to_dataframe(self):
        df0 = pandas.DataFrame(data={'x': ['data_a10_bq']})
        populate()
        df1 = gpl4.load(
            source='bq',
            destination='dataframe',
            data_name='a10_bq')
        self.assertFalse(gpl4.exist_in_bq(data_name='a10_bq'))
        self.assertTrue(df0.equals(df1))

    def test_gs_to_local(self):
        populate_bucket()
        gpl2.load(
            source='gs',
            destination='local',
            data_name='a7',
            delete_in_gs=False)
        self.assertEqual(len(gpl2.list_blob_uris(data_name='a7')), 1)
        self.assertEqual(len(gpl2.list_local_file_paths(data_name='a7')), 1)

    def test_local_to_dataframe(self):
        l0 = ['data_a{}_local'.format(i) for i in range(10, 14)]
        populate_local_folder()
        df1 = gpl5.load(
            source='local',
            destination='dataframe',
            data_name='a1')
        l1 = sorted(list(df1.x))
        self.assertEqual(l0, l1)

    def query_to_dataframe(self):
        df0 = pandas.DataFrame(data={'x': [1, 1]})
        populate()
        df1 = gpl2.load(
            source='query',
            destination='dataframe',
            query='select 1 as x union all select 1 as x',
            data_name='a1',
            delete_in_bq=False)
        self.assertTrue(gpl2.exist_in_bq(data_name='a1'))
        self.assertFalse(gpl2.exist_in_gs(data_name='a1'))
        self.assertFalse(gpl2.exist_in_local(data_name='a1'))
        self.assertTrue(df0.equals(df1))

    def local_to_bq(self):
        populate()
        gpl3.load(
            source='local',
            destination='bq',
            data_name='a',
            bq_schema=[bigquery.SchemaField('x', 'STRING')])
        self.assertFalse(gpl3.exist_in_local(data_name='a'))
        self.assertFalse(gpl3.exist_in_gs(data_name='a'))
        table_ref = dataset_ref.table(table_id='a')
        table = bq_client.get_table(table_ref)
        num_rows = table.num_rows
        self.assertEqual(num_rows, 5)

    def dataframe_to_gs(self):
        df = pandas.DataFrame(data={'x': [1]})
        gpl3.load(
            source='dataframe',
            destination='gs',
            data_name='b',
            dataframe=df,
            delete_in_local=False)
        self.assertTrue(gpl3.exist_in_local(data_name='b'))
        self.assertEqual(len(gpl3.list_blob_uris(data_name='b')), 1)

    def local_to_gs(self):
        populate()
        gpl1.load(
            source='local',
            destination='gs',
            data_name='a1')
        self.assertTrue(gpl1.exist_in_local(data_name='a'))
        self.assertFalse(gpl1.exist_in_local(data_name='a1'))
        self.assertEqual(len(gpl1.list_blob_uris(data_name='a1')), 4)

    def bq_to_query(self):
        populate_dataset()
        query = gpl5.load(
            source='bq',
            destination='query',
            data_name='a8_bq')
        self.assertTrue(gpl5.exist_in_bq(data_name='a8_bq'))
        self.assertEqual(query, 'select * from `{}.{}.{}`'.format(project_id, dataset_id, 'a8_bq'))

    def dataframe_to_query(self):
        l0 = [3, 4, 7]
        df0 = pandas.DataFrame(data={'x': l0})
        populate()
        query = gpl5.load(
            source='dataframe',
            destination='query',
            data_name='a',
            dataframe=df0)
        self.assertFalse(gpl5.exist_in_local(data_name='a'))
        self.assertFalse(gpl5.exist_in_gs(data_name='a'))
        self.assertTrue(gpl5.exist_in_bq(data_name='a'))
        self.assertEqual(query, 'select * from `{}.{}.{}`'.format(project_id, dataset_id, 'a'))
        df1 = bq_client.query(query).to_dataframe()
        l1 = sorted(list(df1.x))
        self.assertEqual(l0, l1)

    def upload_download(self):
        df0 = pandas.DataFrame(data={'x': [1], 'y': [3]})
        populate()
        query = gpl1.load(
            source='dataframe',
            destination='query',
            data_name='a9',
            dataframe=df0)
        df1 = gpl1.load(
            source='query',
            destination='dataframe',
            query=query)
        self.assertTrue(df0.equals(df1))

    def download_upload(self):
        df0 = pandas.DataFrame(data={'x': [3]})
        df1 = gpl2.load(
            source='query',
            destination='dataframe',
            query='select 3 as x')
        self.assertTrue(df0.equals(df1))
        query = gpl2.load(
            source='dataframe',
            destination='query',
            dataframe=df1)
        df2 = bq_client.query(query).to_dataframe()
        self.assertTrue(df0.equals(df2))

    def test_mload(self):
        populate()
        config1 = LoadConfig(
            source='dataframe',
            destination='query',
            data_name='a10',
            dataframe=pandas.DataFrame(data={'x': [3]}))
        config2 = LoadConfig(
            source='query',
            destination='dataframe',
            query='select 4 as y')
        config3 = LoadConfig(
            source='query',
            destination='gs',
            data_name='e0',
            query='select 4 as y')
        load_results = gpl5.mload(configs=[config1, config2, config3])
        self.assertEqual(len(load_results), 3)
        self.assertEqual(load_results[0], 'select * from `{}.{}.a10`'.format(project_id, dataset_id))
        self.assertTrue(load_results[1].equals(pandas.DataFrame(data={'y': [4]})))
        self.assertTrue(load_results[2] is None)

    def test_diamond(self):
        df0 = pandas.DataFrame(data={'x': [3]})
        query = 'select 3 as x'
        populate()
        df1 = gpl5.xload(
            source='query',
            destination='dataframe',
            query=query).load_result
        config = LoadConfig(
            source='query',
            destination='dataframe',
            query=query)
        df2 = gpl5.mload(configs=[config])[0]
        self.assertTrue(df0.equals(df1))
        self.assertTrue(df0.equals(df2))

    def test_config_repeated(self):
        df0 = pandas.DataFrame(data={'x': [3]})
        populate()
        config = LoadConfig(
            source='query',
            destination='dataframe',
            query='select 3 as x')
        dfs = gpl5.mload(configs=[config] * 3)
        for df in dfs:
            self.assertTrue(df0.equals(df))
