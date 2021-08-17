import numpy
import pandas
from google.cloud import bigquery
from google_pandas_load import LoadConfig
from tests.context.resources import project_id, bq_client, \
    dataset_ref, dataset_name
from tests.context.loaders import gpl1, gpl2, gpl3, gpl4, gpl5
from tests.base_class import BaseClassTest
from tests.populate import populate_dataset, populate, populate_bucket, \
    populate_local_folder


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
        self.assertTrue(gpl4.exist_in_bq('a10_bq'))
        self.assertTrue(df0.equals(df1))

    def test_gs_to_local(self):
        populate_bucket()
        gpl2.load(
            source='gs',
            destination='local',
            data_name='a7')
        self.assertEqual(len(gpl2.list_blob_uris('a7')), 1)
        self.assertEqual(len(gpl2.list_local_file_paths('a7')), 1)

    def test_local_to_dataframe(self):
        l0 = ['data_a{}_local'.format(i) for i in range(10, 14)]
        populate_local_folder()
        df1 = gpl5.load(
            source='local',
            destination='dataframe',
            data_name='a1')
        l1 = sorted(list(df1.x))
        self.assertEqual(l0, l1)

    def test_query_to_dataframe(self):
        df0 = pandas.DataFrame(data={'x': [1, 1]})
        populate()
        df1 = gpl2.load(
            source='query',
            destination='dataframe',
            query='select 1 as x union all select 1 as x',
            data_name='a1')
        self.assertFalse(gpl2.exist_in_bq('a1'))
        self.assertFalse(gpl2.exist_in_gs('a1'))
        self.assertFalse(gpl2.exist_in_local('a1'))
        self.assertTrue(df0.equals(df1))

    def test_local_to_bq(self):
        populate()
        gpl3.load(
            source='local',
            destination='bq',
            data_name='a',
            bq_schema=[bigquery.SchemaField('x', 'STRING')])
        self.assertTrue(gpl3.exist_in_local('a'))
        self.assertFalse(gpl3.exist_in_gs('a'))
        table_ref = dataset_ref.table(table_id='a')
        table = bq_client.get_table(table_ref)
        num_rows = table.num_rows
        self.assertEqual(num_rows, 5)

    def test_dataframe_to_gs(self):
        df = pandas.DataFrame(data={'x': [1]})
        gpl3.load(
            source='dataframe',
            destination='gs',
            data_name='b',
            dataframe=df)
        self.assertFalse(gpl3.exist_in_local('b'))
        self.assertEqual(len(gpl3.list_blob_uris('b')), 1)

    def test_local_to_gs(self):
        populate()
        gpl1.load(
            source='local',
            destination='gs',
            data_name='a1')
        self.assertTrue(gpl1.exist_in_local('a1'))
        self.assertEqual(len(gpl1.list_blob_uris('a1')), 4)

    def test_dataframe_to_bq(self):
        l0 = [3, 4, 7]
        df0 = pandas.DataFrame(data={'x': l0})
        populate()
        gpl5.load(
            source='dataframe',
            destination='bq',
            data_name='a',
            dataframe=df0)
        self.assertFalse(gpl5.exist_in_local('a'))
        self.assertFalse(gpl5.exist_in_gs('a'))
        self.assertTrue(gpl5.exist_in_bq('a'))
        query = 'select * from `{}.{}.{}`'.format(
            project_id, dataset_name, 'a')
        df1 = bq_client.query(query).to_dataframe()
        l1 = sorted(list(df1.x))
        self.assertEqual(l0, l1)

    def test_upload_download(self):
        df0 = pandas.DataFrame(data={'x': [1], 'y': [3]})
        populate()
        gpl1.load(
            source='dataframe',
            destination='bq',
            data_name='a9',
            dataframe=df0)
        query = 'select * from `{}.{}.{}`'.format(
            project_id, dataset_name, 'a9')
        df1 = gpl1.load(
            source='query',
            destination='dataframe',
            query=query)
        self.assertTrue(df0.equals(df1))

    def test_download_upload(self):
        df0 = pandas.DataFrame(data={'x': [3]})
        df1 = gpl2.load(
            source='query',
            destination='dataframe',
            query='select 3 as x')
        self.assertTrue(df0.equals(df1))
        gpl2.load(
            source='dataframe',
            destination='bq',
            data_name='b8',
            dataframe=df1)
        query = 'select * from `{}.{}.{}`'.format(
            project_id, dataset_name, 'b8')
        df2 = bq_client.query(query).to_dataframe()
        self.assertTrue(df0.equals(df2))

    def test_mload(self):
        populate()
        config1 = LoadConfig(
            source='dataframe',
            destination='bq',
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
        load_results = gpl5.mload([config1, config2, config3])
        self.assertEqual(len(load_results), 3)
        self.assertTrue(load_results[0] is None)
        df2 = pandas.DataFrame(data={'y': [4]})
        self.assertTrue(load_results[1].equals(df2))
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
        df2 = gpl5.mload([config])[0]
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

    def test_no_skip_blank_lines(self):
        df0 = pandas.DataFrame(data={'x': [3, numpy.nan]})
        df1 = pandas.DataFrame(data={'x': [numpy.nan, 4]})
        df2 = pandas.DataFrame(data={
            'x': [numpy.nan, 5], 'y': [numpy.nan, 6]})
        df3 = pandas.DataFrame(data={
            'x': [7, numpy.nan], 'y': [8, numpy.nan]})
        dfs = [df0, df1, df2, df3]
        populate()
        query0 = 'select 3 as x union all select null as x'
        query1 = 'select null as x union all select 4 as x'
        query2 = 'select null as x, null as y union all ' \
                 'select 5 as x, 6 as y'
        query3 = 'select 7 as x, 8 as y union all ' \
                 'select null as x, null as y'
        queries = [query0, query1, query2, query3]
        configs = []
        for query in queries:
            config = LoadConfig(
                source='query', destination='dataframe', query=query)
            configs.append(config)
        dgs = gpl2.mload(configs)
        for df, dg in zip(dfs, dgs):
            df = df.sort_values('x').reset_index(drop=True)
            dg = dg.sort_values('x').reset_index(drop=True)
            self.assertTrue(dg.equals(df))
