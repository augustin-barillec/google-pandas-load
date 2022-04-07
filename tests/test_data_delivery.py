import numpy
import pandas
from google.cloud import bigquery
from google_pandas_load import LoadConfig
from tests.utils.resources import dataset_id
from tests.utils.populate import populate_dataset, populate_bucket, \
    populate_local, populate
from tests.utils import ids
from tests.utils import load
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


class DataDeliveryTest(BaseClassTest):

    def test_query_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [3, 2], 'y': ['a', 'b']})
        populate_dataset()
        loaders.gpl21.load(
            source='query',
            destination='dataset',
            query="select 3 as x, 'a' as y union all select 2 as x, 'b' as y",
            data_name='a0')
        computed = load.dataset_to_dataframe('a0')
        self.assert_pandas_equal(expected, computed)

    def test_query_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': [3, 2], 'y': ['a', 'b']})
        populate()
        computed = loaders.gpl01.load(
            source='query',
            destination='dataframe',
            query="select 3 as x, 'a' as y union all select 2 as x, 'b' as y")
        self.assert_pandas_equal(expected, computed)

    def test_dataset_to_bucket(self):
        expected = pandas.DataFrame(data={'x': ['a8_dataset']})
        populate_dataset()
        loaders.gpl20.load(
            source='dataset',
            destination='bucket',
            data_name='a8')
        blob_name = ids.build_blob_name_2('a8-000000000000.csv.gz')
        computed = load.bucket_to_dataframe(blob_name, decompress=True)
        self.assert_pandas_equal(expected, computed)

    def test_dataset_to_local(self):
        expected = pandas.DataFrame(data={'x': [1, 2, 3, 4]})
        load.multi_dataframe_to_dataset([expected], ['b1'])
        loaders.gpl21.load(
            source='dataset',
            destination='local',
            data_name='b1')
        local_file_path = ids.build_local_file_path_1('b1-000000000000.csv.gz')
        computed = load.local_to_dataframe(local_file_path)
        self.assert_pandas_equal(expected, computed)

    def test_bucket_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [
            f'a{i}_bucket' for i in range(7, 12)]})
        populate_dataset()
        populate_bucket()
        loaders.gpl01.load(
            source='bucket',
            destination='dataset',
            data_name='a',
            bq_schema=[bigquery.SchemaField(name='x', field_type='STRING')])
        computed = load.dataset_to_dataframe('a')
        self.assert_pandas_equal(expected, computed)

    def test_bucket_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': [3, 2], 'y': ['a', 'b']})
        populate()
        blob_name = ids.build_blob_name_2('a10')
        load.dataframe_to_bucket(expected, blob_name)
        computed = loaders.gpl21.load(
            source='bucket',
            destination='dataframe',
            data_name='a10')
        self.assert_pandas_equal(expected, computed)

    def test_local_to_bucket(self):
        expected = pandas.DataFrame(data={'y': ['c', 'a', 'b']})
        local_file_path = ids.build_local_file_path_0('b')
        load.dataframe_to_local(expected, local_file_path)
        loaders.gpl20.load(
            source='local',
            destination='bucket',
            data_name='b')
        blob_name = ids.build_blob_name_2('b')
        computed = load.bucket_to_dataframe(blob_name, decompress=False)
        self.assert_pandas_equal(expected, computed)

    def test_local_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': [
            f'a{i}_local' for i in range(10, 13)]})
        populate_local()
        computed = loaders.gpl11.load(
            source='local',
            destination='dataframe',
            data_name='a1')
        self.assert_pandas_equal(expected, computed)

    def test_dataframe_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [1, 2, 3], 'y': [1, 2, 4]})
        populate()
        loaders.gpl21.load(
            source='dataframe',
            destination='dataset',
            dataframe=expected,
            data_name='a1')
        computed = load.dataset_to_dataframe('a1')
        self.assert_pandas_equal(expected, computed)

    def test_dataframe_to_bucket(self):
        expected = pandas.DataFrame(data={'x': [1, 2, 3], 'y': [1, 2, 4]})
        populate()
        loaders.gpl00.load(
            source='dataframe',
            destination='bucket',
            dataframe=expected,
            data_name='a1')
        blob_name = ids.build_blob_name_0('a1.csv.gz')
        computed = load.bucket_to_dataframe(blob_name, decompress=True)
        self.assert_pandas_equal(expected, computed)

    def test_upload_download(self):
        expected = pandas.DataFrame(data={'x': [1], 'y': [3]})
        populate()
        loaders.gpl20.load(
            source='dataframe',
            destination='dataset',
            dataframe=expected,
            data_name='a9')
        query = f'select * from {dataset_id}.a9'
        computed = loaders.gpl20.load(
            source='query',
            destination='dataframe',
            query=query)
        self.assert_pandas_equal(expected, computed)

    def test_download_upload(self):
        expected = pandas.DataFrame(data={'x': [3, 2]})
        df0 = loaders.gpl11.load(
            source='query',
            destination='dataframe',
            query='select 3 as x union all select 2 as x')
        loaders.gpl11.load(
            source='dataframe',
            destination='dataset',
            dataframe=df0,
            data_name='b1')
        computed = load.dataset_to_dataframe('b1')
        self.assert_pandas_equal(expected, computed)

    def test_config_repeated(self):
        expected = pandas.DataFrame(data={'x': [3]})
        populate()
        config = LoadConfig(
            source='query',
            destination='dataframe',
            query='select 3 as x')
        computeds = loaders.gpl01.multi_load(configs=[config] * 3)
        for computed in computeds:
            self.assert_pandas_equal(expected, computed)

    def test_heterogeneous_configs(self):
        expected1 = pandas.DataFrame(data={'x': [3, 10]})
        expected2 = pandas.DataFrame(data={'y': [4]})
        expected3 = pandas.DataFrame(data={'x': ['b'], 'y': ['a']})
        populate()
        config1 = LoadConfig(
            source='dataframe',
            destination='dataset',
            dataframe=expected1,
            data_name='a10')
        config2 = LoadConfig(
            source='query',
            destination='dataframe',
            query='select 4 as y')
        config3 = LoadConfig(
            source='query',
            destination='bucket',
            query="select 'b' as x, 'a' as y",
            data_name='a11')
        load_results = loaders.gpl20.multi_load([config1, config2, config3])
        self.assertEqual(len(load_results), 3)
        self.assertTrue(load_results[0] is None)
        self.assertTrue(load_results[2] is None)

        computed1 = load.dataset_to_dataframe('a10')
        self.assert_pandas_equal(expected1, computed1)

        computed2 = load_results[1]
        self.assert_pandas_equal(expected2, computed2)

        blob_name = ids.build_blob_name_2('a11-000000000000.csv.gz')
        computed3 = load.bucket_to_dataframe(
            blob_name, decompress=True)
        self.assert_pandas_equal(expected3, computed3)

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
        computed = loaders.gpl01.multi_load(configs)
        for df, dg in zip(expecteds, computed):
            self.assert_pandas_equal(df, dg)
