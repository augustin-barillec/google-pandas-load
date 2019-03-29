import os
import shutil
import binascii
import unittest
import pandas
from datetime import datetime
from google.cloud import bigquery
from google_pandas_load import LoadConfig
from google_pandas_load.constants import ATOMIC_FUNCTION_NAMES
from tests.context.loaders import gpl1, gpl2, gpl3, gpl4, gpl5, \
    gpl_no_bq_client, gpl_no_dataset_ref, gpl_no_bucket, gpl_no_local_dir_path
from tests.context.common_arguments import project_id, bq_client, dataset_id, dataset_ref, bucket_name, local_dir_path


class LoaderTest(unittest.TestCase):
    def setUp(self):
        if not os.path.isdir(local_dir_path):
            os.makedirs(local_dir_path)

    def tearDown(self):
        shutil.rmtree(local_dir_path)

    def test_simple_back_and_forth(self):
        df0 = pandas.DataFrame(data={'x': [1], 'y': [3]})
        gpl1.load(
            source='dataframe',
            destination='bq',
            data_name='dn',
            dataframe=df0)
        df1 = gpl1.load(
            source='bq',
            destination='dataframe',
            data_name='dn')
        self.assertTrue(df0.equals(df1))

    def test_diamond(self):
        query = 'select 3 as x'
        df0 = gpl5.xload(
            source='query',
            destination='dataframe',
            data_name='dn',
            query=query).load_result
        config = LoadConfig(
            source='query',
            destination='dataframe',
            data_name='dn',
            query=query)
        df1 = gpl5.mload(configs=[config])[0]
        self.assertTrue(df0.equals(df1))

    def test_dtype(self):
        df0 = pandas.DataFrame(data={'x': ['00236'], 'y': [5.0]})
        query = """
        select '00236' as x, 5 as y 
        """
        df1 = gpl2.load(
            source='query',
            destination='dataframe',
            query=query,
            dtype={'x': str, 'y': float})
        self.assertTrue(df0.equals(df1))

    def test_parse_dates(self):
        datetime1 = datetime.strptime('2012-11-14 14:32:30', '%Y-%m-%d %H:%M:%S')
        datetime2 = datetime.strptime('2013-11-14 14:32:30.100121', '%Y-%m-%d %H:%M:%S.%f')
        date1 = datetime1.date()
        df0 = pandas.DataFrame(data={'x': [datetime1],
                                     'y': [datetime2],
                                     'z': [date1]})
        query = """
        select 
        cast('2012-11-14 14:32:30' as TIMESTAMP) as x, 
        '2013-11-14 14:32:30.100121' as y,
        cast('2012-11-14' as DATE) as z
        """
        df1 = gpl2.load(
            source='query',
            destination='dataframe',
            query=query,
            parse_dates=['x', 'y', 'z'],
            infer_datetime_format=True)
        df1['z'] = df1['z'].apply(lambda z: z.date())
        self.assertTrue(df0.equals(df1))

    def test_bq_schema_inferred(self):
        datetime1 = datetime.strptime('2012-11-14 14:32:30', '%Y-%m-%d %H:%M:%S')
        datetime2 = datetime.strptime('2003-11-14 14:32:30.100121', '%Y-%m-%d %H:%M:%S.%f')
        date1 = datetime1.date()
        date2 = datetime2.date()
        df0 = pandas.DataFrame(data={'a': [date1, date2],
                                     'b': [datetime1, datetime2],
                                     'c': [True, False],
                                     'd': [4, 3],
                                     'e': [1.1, 2.2],
                                     'f': ['1', '2']})
        gpl3.load(
            source='dataframe',
            destination='bq',
            data_name='dn',
            dataframe=df0,
            date_cols=['a'],
            timestamp_cols=['b'])
        table_ref = dataset_ref.table(table_id='dn')
        table = bq_client.get_table(table_ref=table_ref)
        f1, f2, f3, f4, f5, f6 = table.schema
        self.assertEqual((f1.name, f1.field_type), ('a', 'DATE'))
        self.assertEqual((f2.name, f2.field_type), ('b', 'TIMESTAMP'))
        self.assertEqual((f3.name, f3.field_type), ('c', 'BOOLEAN'))
        self.assertEqual((f4.name, f4.field_type), ('d', 'INTEGER'))
        self.assertEqual((f5.name, f5.field_type), ('e', 'FLOAT'))
        self.assertEqual((f6.name, f6.field_type), ('f', 'STRING'))
        gpl3.delete_in_bq(data_name='e0')

    def test_bq_schema_given(self):
        df0 = pandas.DataFrame(data={'x': ['1'], 'y': [3]})
        bq_schema = [bigquery.SchemaField(name='x', field_type='FLOAT'),
                     bigquery.SchemaField(name='y', field_type='FLOAT')]
        gpl4.load(
            source='dataframe',
            destination='bq',
            data_name='dn1',
            dataframe=df0,
            bq_schema=bq_schema)
        table_ref = dataset_ref.table(table_id='dn1')
        table = bq_client.get_table(table_ref=table_ref)
        f1, f2 = table.schema
        self.assertEqual((f1.name, f1.field_type), ('x', 'FLOAT'))
        self.assertEqual((f2.name, f2.field_type), ('y', 'FLOAT'))
        gpl4.delete_in_bq(data_name='e0')

    def test_exist_delete_methods(self):
        gpl1.load(source='query', destination='bq', data_name='e130', query='select 3')
        self.assertTrue(gpl1.exist_in_bq(data_name='e130'))
        gpl1.delete_in_bq(data_name='e130')
        self.assertFalse(gpl1.exist_in_bq(data_name='e130'))

        gpl2.load(source='query', destination='gs', data_name='e131', query='select 3')
        self.assertTrue(gpl2.exist_in_gs(data_name='e131'))
        gpl2.delete_in_gs(data_name='e131')
        self.assertFalse(gpl2.exist_in_gs(data_name='e131'))

        gpl3.load(source='query', destination='local', data_name='e133', query='select 3')
        self.assertTrue(gpl3.exist_in_local(data_name='e133'))
        gpl3.delete_in_local(data_name='e133')
        self.assertFalse(gpl3.exist_in_local(data_name='e133'))

    def test_delete_arg_false(self):
        df0 = pandas.DataFrame(data={'x': [1], 'y': [3]})
        xlr = gpl3.xload(
            source='dataframe',
            destination='query',
            dataframe=df0,
            delete_in_bq=False,
            delete_in_gs=False,
            delete_in_local=False)
        query = xlr.load_result
        data_name_0 = xlr.data_name
        self.assertTrue(gpl3.exist_in_bq(data_name=data_name_0))
        self.assertTrue(gpl3.exist_in_gs(data_name=data_name_0))
        self.assertTrue(gpl3.exist_in_local(data_name=data_name_0))
        xlr = gpl3.xload(
            source='query',
            destination='dataframe',
            query=query,
            delete_in_bq=False,
            delete_in_gs=False,
            delete_in_local=False)
        df1 = xlr.load_result
        data_name_1 = xlr.data_name
        self.assertTrue(gpl3.exist_in_bq(data_name=data_name_1))
        self.assertTrue(gpl3.exist_in_gs(data_name=data_name_1))
        self.assertTrue(gpl3.exist_in_local(data_name=data_name_1))
        self.assertTrue(df0.equals(df1))
        for data_name in [data_name_0, data_name_1]:
            gpl3.delete_in_bq(data_name=data_name)
            gpl3.delete_in_gs(data_name=data_name)
            gpl3.delete_in_local(data_name=data_name)

    def test_delete_arg_true(self):
        df0 = pandas.DataFrame(data={'x': [1], 'y': [3]})
        xlr = gpl3.xload(
            source='dataframe',
            destination='query',
            dataframe=df0,
            delete_in_gs=True,
            delete_in_local=True)
        query = xlr.load_result
        data_name_0 = xlr.data_name
        self.assertTrue(gpl3.exist_in_bq(data_name=data_name_0))
        self.assertFalse(gpl3.exist_in_gs(data_name=data_name_0))
        self.assertFalse(gpl3.exist_in_local(data_name=data_name_0))
        xlr = gpl3.xload(
            source='query',
            destination='dataframe',
            query=query,
            delete_in_bq=True,
            delete_in_gs=True,
            delete_in_local=True)
        df1 = xlr.load_result
        data_name_1 = xlr.data_name
        self.assertFalse(gpl3.exist_in_bq(data_name=data_name_1))
        self.assertFalse(gpl3.exist_in_gs(data_name=data_name_1))
        self.assertFalse(gpl3.exist_in_local(data_name=data_name_1))
        self.assertTrue(df0.equals(df1))
        for data_name in [data_name_0, data_name_1]:
            gpl3.delete_in_bq(data_name=data_name)
            gpl3.delete_in_gs(data_name=data_name)
            gpl3.delete_in_local(data_name=data_name)

    def test_prefix_in_gs(self):
        df0 = pandas.DataFrame(data={'x': [5], 'y': [7]})
        df1 = pandas.DataFrame(data={'x': [5, 5], 'y': [7, 7]})
        configs = []
        for i in range(2):
            config = LoadConfig(
                source='dataframe',
                destination='gs',
                data_name='e8{}'.format(i),
                dataframe=df0)
            configs.append(config)
        gpl5.mload(configs=configs)
        df2 = gpl5.load(source='gs', destination='dataframe', data_name='e8', delete_in_gs=False)
        df2.reset_index(drop=True, inplace=True)
        self.assertTrue(df2.equals(df1))
        gpl5.load(source='gs', destination='bq', data_name='e8',
                  bq_schema=[bigquery.SchemaField('x', 'INTEGER'), bigquery.SchemaField('y', 'INTEGER')])
        df3 = gpl5.load(source='bq', destination='dataframe', data_name='e8')
        self.assertTrue(df3.equals(df1))

    def test_prefix_in_local(self):
        df0 = pandas.DataFrame(data={'x': [5], 'y': [7]})
        df1 = pandas.DataFrame(data={'x': [5, 5], 'y': [7, 7]})
        configs = []
        for i in range(2):
            config = LoadConfig(
                source='dataframe',
                destination='local',
                data_name='e8{}'.format(i),
                dataframe=df0)
            configs.append(config)
        gpl1.mload(configs=configs)
        df2 = gpl1.load(source='local', destination='dataframe', data_name='e8', delete_in_local=False)
        df2.reset_index(drop=True, inplace=True)
        self.assertTrue(df2.equals(df1))
        gpl1.load(source='local', destination='bq', data_name='e8',
                  bq_schema=[bigquery.SchemaField('x', 'INTEGER'), bigquery.SchemaField('y', 'INTEGER')])
        df3 = gpl1.load(source='bq', destination='dataframe', data_name='e8')
        self.assertTrue(df3.equals(df1))

    def test_compress(self):

        def is_gz_file(filepath):
            with open(filepath, 'rb') as test_f:
                return binascii.hexlify(test_f.read(2)) == b'1f8b'

        gpl2.load(
            source='query',
            destination='local',
            data_name='e110',
            query='select 5')
        local_file_path = gpl1.list_local_file_paths(data_name='e110')[0]
        self.assertTrue(is_gz_file(local_file_path))

        gpl2.load(
            source='dataframe',
            destination='local',
            data_name='e111',
            dataframe=pandas.DataFrame(data={'x': [1]}))
        local_file_path = gpl1.list_local_file_paths(data_name='e111')[0]
        self.assertTrue(is_gz_file(local_file_path))

        gpl4.load(
            source='query',
            destination='local',
            data_name='e112',
            query='select 5')
        local_file_path = gpl1.list_local_file_paths(data_name='e112')[0]
        self.assertFalse(is_gz_file(local_file_path))

        gpl4.load(
            source='dataframe',
            destination='local',
            data_name='e113',
            dataframe=pandas.DataFrame(data={'x': [1]}))
        local_file_path = gpl1.list_local_file_paths(data_name='e113')[0]
        self.assertFalse(is_gz_file(local_file_path))

        for data_name in ['e110', 'e111', 'e112', 'e113']:
            gpl5.delete_in_local(data_name=data_name)

    def test_wildcard(self):
        gpl1.load(source='query', destination='gs', data_name='e0', query='select 2')
        list_blob_uris = gpl1.list_blob_uris(data_name='e0')
        self.assertEqual(list_blob_uris, ['gs://{}/e0-000000000000.csv.gz'.format(bucket_name)])
        gpl1.delete_in_gs(data_name='e0')

    def test_mload(self):
        config1 = LoadConfig(
            source='dataframe',
            destination='query',
            data_name='e0',
            dataframe=pandas.DataFrame(data={'x': [3]}))
        config2 = LoadConfig(
            source='query',
            destination='dataframe',
            query='select 4 as y')
        config3 = LoadConfig(
            source='query',
            destination='gs',
            data_name='e1',
            query='select 4 as y')
        mlr = gpl5.mload(configs=[config1, config2, config3])
        self.assertEqual(len(mlr), 3)
        self.assertEqual(mlr[0], 'select * from `{}.{}.e0`'.format(project_id, dataset_id))
        self.assertTrue(mlr[1].equals(pandas.DataFrame(data={'y': [4]})))
        self.assertTrue(mlr[2] is None)
        gpl5.delete_in_gs(data_name='e1')

    def test_xload(self):
        xlr = gpl3.xload(source='query', destination='dataframe', query='select 3')

        self.assertEqual(set(vars(xlr)), {'load_result', 'data_name', 'duration', 'durations', 'query_cost'})

        self.assertEqual(type(xlr.data_name), str)

        self.assertTrue(xlr.duration > 0)

        self.assertEqual(set(vars(xlr.durations)), set(ATOMIC_FUNCTION_NAMES))

        for n in ATOMIC_FUNCTION_NAMES:
            duration = vars(xlr.durations)[n]
            if duration is not None:
                self.assertTrue(duration >= 0)

        self.assertEqual(xlr.query_cost, 0.0)

    def test_xmload(self):
        df0 = pandas.DataFrame(data={'x': [4]})
        config1 = LoadConfig(
            source='dataframe',
            destination='query',
            data_name='e100',
            dataframe=df0)
        config2 = LoadConfig(
            source='query',
            destination='bq',
            data_name='e101',
            query='select 3')
        config3 = LoadConfig(
            source='dataframe',
            destination='query',
            data_name='e102',
            dataframe=df0)
        xmlr = gpl3.xmload(configs=[config1, config2, config3])
        self.assertEqual(
            set(vars(xmlr)),
            {'load_results', 'data_names', 'duration', 'durations', 'query_cost', 'query_costs'})

        self.assertEqual(xmlr.data_names, ['e100', 'e101', 'e102'])

        self.assertTrue(xmlr.duration > 0)

        self.assertEqual(set(vars(xmlr.durations)), set(ATOMIC_FUNCTION_NAMES))

        for n in ATOMIC_FUNCTION_NAMES:
            duration = vars(xmlr.durations)[n]
            if duration is not None:
                self.assertTrue(duration >= 0)

        self.assertEqual(xmlr.query_cost, 0.0)

        self.assertEqual(xmlr.query_costs, [None, 0.0, None])

        for data_name in ['e100', 'e101', 'e102']:
            gpl3.delete_in_bq(data_name=data_name)

    def test_raise_error_if_no_data(self):
        gpl1.delete_in_bq(data_name='e0', warn=False)
        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='bq', destination='local', data_name='e0')
        self.assertEqual(str(cm.exception), 'There is no data named e0 in bq')
        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='bq', destination='query', data_name='e0')
        self.assertEqual(str(cm.exception), 'There is no data named e0 in bq')

        gpl1.delete_in_gs(data_name='e0', warn=False)
        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='gs', destination='local', data_name='e0')
        self.assertEqual(str(cm.exception), 'There is no data named e0 in gs')
        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='gs', destination='bq', data_name='e0',
                      bq_schema=[bigquery.SchemaField('x', 'INTEGER')])
        self.assertEqual(str(cm.exception), 'There is no data named e0 in gs')

        gpl1.delete_in_local(data_name='e0', warn=False)
        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='local', destination='dataframe', data_name='e0')
        self.assertEqual(str(cm.exception), 'There is no data named e0 in local')
        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='local', destination='gs', data_name='e0')
        self.assertEqual(str(cm.exception), 'There is no data named e0 in local')

    def test_raise_error_if_prefix(self):
        config1 = LoadConfig(
            source='dataframe',
            destination='query',
            data_name='a',
            dataframe=pandas.DataFrame(data={'x': [3]}))
        config2 = LoadConfig(
            source='query',
            destination='dataframe',
            data_name='aa',
            query='select 4 as y')
        with self.assertRaises(ValueError) as cm:
            gpl2.mload(configs=[config1, config2])
        self.assertEqual(str(cm.exception), 'a is a prefix of aa')

    def test_config_repeated(self):
        df0 = pandas.DataFrame(data={'x': [3]})
        config = LoadConfig(
            source='query',
            destination='dataframe',
            query='select 3 as x')
        dfs = gpl5.mload(configs=[config]*3)
        for df in dfs:
            self.assertTrue(df0.equals(df))

    def test_raise_error_if_missing_required_resources(self):
        with self.assertRaises(ValueError) as cm:
            gpl_no_bq_client.load(source='query', destination='bq', query='select 3', data_name='e0')
        self.assertEqual(str(cm.exception), 'bq_client must be given if bq is used')

        with self.assertRaises(ValueError) as cm:
            gpl_no_dataset_ref.load(source='query', destination='bq', query='select 3', data_name='e0')
        self.assertEqual(str(cm.exception), 'dataset_ref must be given if bq is used')

        with self.assertRaises(ValueError) as cm:
            gpl_no_bucket.load(source='gs', destination='local', data_name='e0')
        self.assertEqual(str(cm.exception), 'bucket must be given if gs is used')

        with self.assertRaises(ValueError) as cm:
            gpl_no_local_dir_path.load(source='local', destination='gs', data_name='e0')
        self.assertEqual(str(cm.exception), 'local_dir_path must be given if local is used')

    def test_raise_error_if_invalid_query(self):
        with self.assertRaises(RuntimeError):
            gpl1.load(source='query', destination='dataframe', query='selectt 3')

