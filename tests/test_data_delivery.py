import os
import numpy
import pandas
from google.cloud import bigquery
from google_pandas_load import LoadConfig
from tests.utils.df_equal import normalize_equal
from tests.utils.resources import project_id, bq_client, \
    dataset_id, dataset_name, local_subdir_path
from tests.utils.populate import populate_bq, populate_gs, \
    populate_local, populate
from tests.utils import ids
from tests.utils import load
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


class DataDeliveryTest(BaseClassTest):

    def test_query_to_bq(self):
        expected = pandas.DataFrame(data={'x': [3, 2], 'y': ['a', 'b']})
        populate_bq()
        loaders.gpl21.load(
            source='query',
            destination='bq',
            data_name='a0',
            query="select 3 as x, 'a' as y union all select 2 as x, 'b' as y")
        computed = load.bq_to_dataframe('a0')
        self.assertTrue(normalize_equal(expected, computed))

    def test_bq_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': ['a10']})
        populate()
        computed = loaders.gpl00.load(
            source='bq',
            destination='dataframe',
            data_name='a10')
        self.assertTrue(expected.equals(computed))

    def test_gs_to_local(self):
        expected = pandas.DataFrame(data={'x': ['a9']})
        populate_gs()
        populate_local()
        loaders.gpl21.load(
            source='gs',
            destination='local',
            data_name='a9')
        local_file_path = ids.build_local_file_path_1('a9')
        computed = load.local_to_dataframe(local_file_path)
        self.assertTrue(expected.equals(computed))

    def test_local_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': [
            f'a{i}' for i in range(10, 13)]})
        populate_local()
        computed = loaders.gpl01.load(
            source='local',
            destination='dataframe',
            data_name='a1')
        self.assertTrue(normalize_equal(expected, computed))

    def test_query_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': [1, 1]})
        populate()
        computed = loaders.gpl21.load(
            source='query',
            destination='dataframe',
            query='select 1 as x union all select 1 as x',
            data_name='a1')
        self.assertTrue(expected.equals(computed))

    def test_local_to_bq(self):
        expected = pandas.DataFrame(data={'x': [
            f'a{i}' for i in range(8, 13)]})
        populate()
        loaders.gpl01.load(
            source='local',
            destination='bq',
            data_name='a',
            bq_schema=[bigquery.SchemaField('x', 'STRING')])
        computed = load.bq_to_dataframe('a')
        self.assertTrue(normalize_equal(expected, computed))

    def test_dataframe_to_gs(self):
        expected = pandas.DataFrame(data={'x': [1, 2], 'y': [2, 1]})
        loaders.gpl01.load(
            source='dataframe',
            destination='gs',
            data_name='b',
            dataframe=expected)
        blob_name = ids.build_blob_name_0('b.csv.gz')
        computed = load.gs_to_dataframe(blob_name)
        self.assertTrue(expected.equals(computed))


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
        gpl6.load(
            source='dataframe',
            destination='bq',
            data_name='a',
            dataframe=df0)
        self.assertFalse(gpl6.exist_in_local('a'))
        self.assertFalse(gpl6.exist_in_gs('a'))
        self.assertTrue(gpl6.exist_in_bq('a'))
        query = f'select * from {project_id}.{dataset_name}.a'
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
        query = f'select * from {project_id}.{dataset_name}.a9'
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
        query = f'select * from {project_id}.{dataset_name}.b8'
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
