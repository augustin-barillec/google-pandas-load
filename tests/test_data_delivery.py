import numpy
import pandas
import google_pandas_load
from google.cloud import bigquery
from tests import utils


class DataDeliveryTest(utils.base_class.BaseClassTest):
    def test_query_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [3, 2], 'y': ['a', 'b']})
        utils.populate.populate_dataset()
        gpl = utils.loader.create_loader(
            gs_client=None,
            bucket_name=None)
        gpl.load(
            source='query',
            destination='dataset',
            query="select 3 as x, 'a' as y union all select 2 as x, 'b' as y",
            data_name='a0')
        computed = utils.load.dataset_to_dataframe('a0')
        self.assert_pandas_equal(expected, computed)

    def test_query_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': [3, 2], 'y': ['a', 'b']})
        utils.populate.populate()
        gpl = utils.loader.create_loader(separator='#')
        computed = gpl.load(
            source='query',
            destination='dataframe',
            query="select 3 as x, 'a' as y union all select 2 as x, 'b' as y")
        self.assert_pandas_equal(expected, computed)

    def test_dataset_to_bucket(self):
        expected = pandas.DataFrame(data={'x': ['a8_dataset']})
        utils.populate.populate_dataset()
        gpl = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_subdir_path,
            local_dir_path=None)
        gpl.load(
            source='dataset',
            destination='bucket',
            data_name='a8')
        blob_name = utils.ids.build_blob_name_2('a8-000000000000.csv.gz')
        computed = utils.load.bucket_to_dataframe(blob_name, decompress=True)
        self.assert_pandas_equal(expected, computed)

    def test_dataset_to_local(self):
        expected = pandas.DataFrame(data={'x': [1, 2, 3, 4]})
        utils.load.multi_dataframe_to_dataset([expected], ['b1'])
        gpl = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_subdir_path,
            local_dir_path=utils.constants.local_subdir_path)
        gpl.load(
            source='dataset',
            destination='local',
            data_name='b1')
        local_file_path = utils.ids.build_local_file_path_1(
            'b1-000000000000.csv.gz')
        computed = utils.load.local_to_dataframe(local_file_path)
        self.assert_pandas_equal(expected, computed)

    def test_bucket_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [
            f'a{i}_bucket' for i in range(7, 12)]})
        utils.populate.populate_dataset()
        utils.populate.populate_bucket()
        gpl = utils.loader.create_loader_quick_setup(local_dir_path=None)
        gpl.load(
            source='bucket',
            destination='dataset',
            data_name='a',
            bq_schema=[bigquery.SchemaField(name='x', field_type='STRING')])
        computed = utils.load.dataset_to_dataframe('a')
        self.assert_pandas_equal(expected, computed)

    def test_bucket_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': [3, 2], 'y': ['a', 'b']})
        utils.populate.populate()
        blob_name = utils.ids.build_blob_name_2('a10')
        utils.load.dataframe_to_bucket(expected, blob_name)
        gpl = utils.loader.create_loader(
            bq_client=None,
            dataset_id=None,
            bucket_dir_path=utils.constants.bucket_subdir_path,
            local_dir_path=utils.constants.local_subdir_path)
        computed = gpl.load(
            source='bucket',
            destination='dataframe',
            data_name='a10')
        self.assert_pandas_equal(expected, computed)

    def test_local_to_bucket(self):
        expected = pandas.DataFrame(data={'y': ['c', 'a', 'b']})
        local_file_path = utils.ids.build_local_file_path_0('b')
        utils.load.dataframe_to_local(expected, local_file_path)
        gpl = utils.loader.create_loader_quick_setup(
            dataset_name=None,
            bucket_dir_path=utils.constants.bucket_subdir_path)
        gpl.load(
            source='local',
            destination='bucket',
            data_name='b')
        blob_name = utils.ids.build_blob_name_2('b')
        computed = utils.load.bucket_to_dataframe(blob_name, decompress=False)
        self.assert_pandas_equal(expected, computed)

    def test_local_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': [
            f'a{i}_local' for i in range(10, 13)]})
        utils.populate.populate_local()
        gpl = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_dir_path,
            local_dir_path=utils.constants.local_subdir_path)
        computed = gpl.load(
            source='local',
            destination='dataframe',
            data_name='a1')
        self.assert_pandas_equal(expected, computed)

    def test_dataframe_to_dataset(self):
        expected = pandas.DataFrame(data={'x': [1, 2, 3], 'y': [1, 2, 4]})
        utils.populate.populate()
        gpl = utils.loader.create_loader_quick_setup()
        gpl.load(
            source='dataframe',
            destination='dataset',
            dataframe=expected,
            data_name='a1')
        computed = utils.load.dataset_to_dataframe('a1')
        self.assert_pandas_equal(expected, computed)

    def test_dataframe_to_bucket(self):
        expected = pandas.DataFrame(data={'x': [1, 2, 3], 'y': [1, 2, 4]})
        utils.populate.populate()
        gpl = utils.loader.create_loader()
        gpl.load(
            source='dataframe',
            destination='bucket',
            dataframe=expected,
            data_name='a1')
        blob_name = utils.ids.build_blob_name_0('a1.csv.gz')
        computed = utils.load.bucket_to_dataframe(blob_name, decompress=True)
        self.assert_pandas_equal(expected, computed)

    def test_upload_download(self):
        expected = pandas.DataFrame(data={'x': [1], 'y': [3]})
        utils.populate.populate()
        gpl = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_subdir_path,
            separator='#',
            chunk_size=2**18,
            timeout=15)
        gpl.load(
            source='dataframe',
            destination='dataset',
            dataframe=expected,
            data_name='a9')
        query = f'select * from {utils.constants.dataset_id}.a9'
        computed = gpl.load(
            source='query',
            destination='dataframe',
            query=query)
        self.assert_pandas_equal(expected, computed)

    def test_download_upload(self):
        expected = pandas.DataFrame(data={'x': [3, 2]})
        gpl = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_dir_path,
            local_dir_path=utils.constants.local_subdir_path)
        df0 = gpl.load(
            source='query',
            destination='dataframe',
            query='select 3 as x union all select 2 as x')
        gpl.load(
            source='dataframe',
            destination='dataset',
            dataframe=df0,
            data_name='b1')
        computed = utils.load.dataset_to_dataframe('b1')
        self.assert_pandas_equal(expected, computed)

    def test_config_repeated(self):
        expected = pandas.DataFrame(data={'x': [3]})
        utils.populate.populate()
        config = google_pandas_load.LoadConfig(
            source='query',
            destination='dataframe',
            query='select 3 as x')
        gpl = utils.loader.create_loader_quick_setup(
            local_dir_path=utils.constants.local_subdir_path)
        computeds = gpl.multi_load(configs=[config] * 3)
        for computed in computeds:
            self.assert_pandas_equal(expected, computed)

    def test_heterogeneous_configs(self):
        expected1 = pandas.DataFrame(data={'x': [3, 10]})
        expected2 = pandas.DataFrame(data={'y': [4]})
        expected3 = pandas.DataFrame(data={'x': ['b'], 'y': ['a']})
        utils.populate.populate()
        config1 = google_pandas_load.LoadConfig(
            source='dataframe',
            destination='dataset',
            dataframe=expected1,
            data_name='a10')
        config2 = google_pandas_load.LoadConfig(
            source='query',
            destination='dataframe',
            query='select 4 as y')
        config3 = google_pandas_load.LoadConfig(
            source='query',
            destination='bucket',
            query="select 'b' as x, 'a' as y",
            data_name='a11')
        gpl = utils.loader.create_loader(
            bucket_dir_path=utils.constants.bucket_subdir_path)
        load_results = gpl.multi_load([config1, config2, config3])
        self.assertEqual(len(load_results), 3)
        self.assertTrue(load_results[0] is None)
        self.assertTrue(load_results[2] is None)

        computed1 = utils.load.dataset_to_dataframe('a10')
        self.assert_pandas_equal(expected1, computed1)

        computed2 = load_results[1]
        self.assert_pandas_equal(expected2, computed2)

        blob_name = utils.ids.build_blob_name_2('a11-000000000000.csv.gz')
        computed3 = utils.load.bucket_to_dataframe(
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
        utils.populate.populate()
        query0 = 'select 3 as x union all select null as x'
        query1 = 'select null as x union all select 4 as x'
        query2 = 'select null as x, null as y union all ' \
                 'select 5 as x, 6 as y'
        query3 = 'select 7 as x, 8 as y union all ' \
                 'select null as x, null as y'
        queries = [query0, query1, query2, query3]
        configs = []
        for query in queries:
            config = google_pandas_load.LoadConfig(
                source='query', destination='dataframe', query=query)
            configs.append(config)
        gpl = utils.loader.create_loader()
        computed = gpl.multi_load(configs)
        for df, dg in zip(expecteds, computed):
            self.assert_pandas_equal(df, dg)
