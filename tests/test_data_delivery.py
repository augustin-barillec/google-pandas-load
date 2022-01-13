import numpy
import pandas
from google.cloud import bigquery
from google_pandas_load import LoadConfig
from tests.utils.df_equal import normalize_equal
from tests.utils.resources import dataset_id
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

    def test_query_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': [3, 2], 'y': ['a', 'b']})
        populate()
        computed = loaders.gpl01.load(
            source='query',
            destination='dataframe',
            query="select 3 as x, 'a' as y union all select 2 as x, 'b' as y")
        self.assertTrue(normalize_equal(expected, computed))

    def test_bq_to_gs(self):
        expected = pandas.DataFrame(data={'x': ['a8_bq']})
        populate_bq()
        loaders.gpl20.load(
            source='bq',
            destination='gs',
            data_name='a8')
        blob_name = ids.build_blob_name_2('a8-000000000000.csv.gz')
        computed = load.gs_to_dataframe(blob_name, decompress=True)
        self.assertTrue(expected.equals(computed))

    def test_bq_to_local(self):
        expected = pandas.DataFrame(data={'x': [1, 2, 3, 4]})
        load.multi_dataframe_to_bq([expected], ['b1'])
        loaders.gpl21.xload(
            source='bq',
            destination='local',
            data_name='b1')
        local_file_path = ids.build_local_file_path_1('b1-000000000000.csv.gz')
        computed = load.local_to_dataframe(local_file_path)
        self.assertTrue(normalize_equal(expected, computed))

    def test_gs_to_bq(self):
        expected = pandas.DataFrame(data={'x': [
            f'a{i}_gs' for i in range(7, 12)]})
        populate_bq()
        populate_gs()
        loaders.gpl01.load(
            source='gs',
            destination='bq',
            data_name='a',
            bq_schema=[bigquery.SchemaField(name='x', field_type='STRING')])
        computed = load.bq_to_dataframe('a')
        self.assertTrue(normalize_equal(expected, computed))

    def test_gs_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': [3, 2], 'y': ['a', 'b']})
        populate()
        blob_name = ids.build_blob_name_2('a10')
        load.dataframe_to_gs(expected, blob_name)
        computed = loaders.gpl21.load(
            source='gs',
            destination='dataframe',
            data_name='a10')
        self.assertTrue(normalize_equal(expected, computed))

    def test_local_to_gs(self):
        expected = pandas.DataFrame(data={'y': ['c', 'a', 'b']})
        local_file_path = ids.build_local_file_path_0('b')
        load.dataframe_to_local(expected, local_file_path)
        loaders.gpl20.load(
            source='local',
            destination='gs',
            data_name='b')
        blob_name = ids.build_blob_name_2('b')
        computed = load.gs_to_dataframe(blob_name, decompress=False)
        self.assertTrue(normalize_equal(expected, computed))

    def test_local_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': [
            f'a{i}_local' for i in range(10, 13)]})
        populate_local()
        computed = loaders.gpl11.xload(
            source='local',
            destination='dataframe',
            data_name='a1').load_result
        self.assertTrue(normalize_equal(expected, computed))

    def test_dataframe_to_bq(self):
        expected = pandas.DataFrame(data={'x': [1, 2, 3], 'y': [1, 2, 4]})
        populate()
        loaders.gpl21.load(
            source='dataframe',
            destination='bq',
            dataframe=expected,
            data_name='a1')
        computed = load.bq_to_dataframe('a1')
        self.assertTrue(normalize_equal(expected, computed))

    def test_dataframe_to_gs(self):
        expected = pandas.DataFrame(data={'x': [1, 2, 3], 'y': [1, 2, 4]})
        populate()
        loaders.gpl00.xload(
            source='dataframe',
            destination='gs',
            dataframe=expected,
            data_name='a1')
        blob_name = ids.build_blob_name_0('a1.csv.gz')
        computed = load.gs_to_dataframe(blob_name, decompress=True)
        self.assertTrue(normalize_equal(expected, computed))

    def test_upload_download(self):
        expected = pandas.DataFrame(data={'x': [1], 'y': [3]})
        populate()
        loaders.gpl20.load(
            source='dataframe',
            destination='bq',
            data_name='a9',
            dataframe=expected)
        query = f'select * from {dataset_id}.a9'
        computed = loaders.gpl20.load(
            source='query',
            destination='dataframe',
            query=query)
        self.assertTrue(expected.equals(computed))

    def test_download_upload(self):
        expected = pandas.DataFrame(data={'x': [3, 2]})
        df0 = loaders.gpl11.load(
            source='query',
            destination='dataframe',
            query='select 3 as x union all select 2 as x')
        loaders.gpl11.load(
            source='dataframe',
            destination='bq',
            dataframe=df0,
            data_name='b1')
        computed = load.bq_to_dataframe('b1')
        self.assertTrue(normalize_equal(expected, computed))

    def test_diamond(self):
        expected = pandas.DataFrame(data={'x': [3]})
        query = 'select 3 as x'
        populate()
        computed1 = loaders.gpl01.xload(
            source='query',
            destination='dataframe',
            query=query).load_result
        config = LoadConfig(
            source='query',
            destination='dataframe',
            query=query)
        computed2 = loaders.gpl10.mload([config])[0]
        self.assertTrue(expected.equals(computed1))
        self.assertTrue(expected.equals(computed2))

    def test_config_repeated(self):
        expected = pandas.DataFrame(data={'x': [3]})
        populate()
        config = LoadConfig(
            source='query',
            destination='dataframe',
            query='select 3 as x')
        computeds = loaders.gpl01.xmload(configs=[config] * 3).load_results
        for computed in computeds:
            self.assertTrue(expected.equals(computed))

    def test_heterogeneous_configs(self):
        expected1 = pandas.DataFrame(data={'x': [3, 10]})
        expected2 = pandas.DataFrame(data={'y': [4]})
        expected3 = pandas.DataFrame(data={'x': ['b'], 'y': ['a']})
        populate()
        config1 = LoadConfig(
            source='dataframe',
            destination='bq',
            data_name='a10',
            dataframe=expected1)
        config2 = LoadConfig(
            source='query',
            destination='dataframe',
            query='select 4 as y')
        config3 = LoadConfig(
            source='query',
            destination='gs',
            data_name='a11',
            query="select 'b' as x, 'a' as y")
        load_results = loaders.gpl20.mload([config1, config2, config3])
        self.assertEqual(len(load_results), 3)
        self.assertTrue(load_results[0] is None)
        self.assertTrue(load_results[2] is None)

        computed1 = load.bq_to_dataframe('a10')
        self.assertTrue(normalize_equal(expected1, computed1))

        computed2 = load_results[1]
        self.assertTrue(expected2.equals(computed2))

        blob_name = ids.build_blob_name_2('a11-000000000000.csv.gz')
        computed3 = load.gs_to_dataframe(
            blob_name, decompress=True)
        self.assertTrue(expected3.equals(computed3))

    def test_no_skip_blank_lines(self):
        df0 = pandas.DataFrame(data={'x': [3, numpy.nan]})
        df1 = pandas.DataFrame(data={'x': [numpy.nan, 4]})
        df2 = pandas.DataFrame(data={
            'x': [numpy.nan, 5], 'y': [numpy.nan, 6]})
        df3 = pandas.DataFrame(data={
            'x': [7, numpy.nan], 'y': [8, numpy.nan]})
        expecteds = [df0, df1, df2, df3]
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
        computed = loaders.gpl01.xmload(configs).load_results
        for df, dg in zip(expecteds, computed):
            self.assertTrue(normalize_equal(df, dg))
