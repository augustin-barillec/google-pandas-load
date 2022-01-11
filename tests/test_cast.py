import pytz
import numpy
import pandas
from datetime import datetime
from google.cloud import bigquery
from tests.utils.resources import bq_client
from tests.utils import ids
from tests.utils import load
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


class CastTest(BaseClassTest):

    def test_dtype_query_to_dataframe(self):
        expected = pandas.DataFrame(data={'x': ['236'], 'y': [5.0]})
        query = """
        select 236 as x, 5 as y 
        """
        computed = loaders.gpl00.load(
            source='query',
            destination='dataframe',
            query=query,
            dtype={'x': str, 'y': float})
        self.assertTrue(expected.equals(computed))

    def test_parse_dates_query_to_dataframe(self):
        datetime1 = datetime(2012, 11, 14, 14, 32, 30, tzinfo=pytz.UTC)
        datetime2 = datetime(2013, 11, 14, 14, 32, 30, 100121)
        date1 = datetime1.date()
        expected = pandas.DataFrame(data={
            'x': [datetime1], 'y': [datetime2], 'z': [date1]})
        query = """
        select 
        cast('2012-11-14 14:32:30' as TIMESTAMP) as x, 
        '2013-11-14 14:32:30.100121' as y,
        cast('2012-11-14' as DATE) as z
        """
        computed = loaders.gpl20.load(
            source='query',
            destination='dataframe',
            query=query,
            parse_dates=['x', 'y', 'z'])
        computed['z'] = computed['z'].apply(lambda z: z.date())
        self.assertTrue(expected.equals(computed))

    def test_bq_schema_inferred_with_source_dataframe(self):
        datetime1 = datetime.strptime('2012-11-14 14:32:30',
                                      '%Y-%m-%d %H:%M:%S')
        datetime2 = datetime.strptime('2003-11-14 14:32:30.100121',
                                      '%Y-%m-%d %H:%M:%S.%f')
        date1 = datetime1.date()
        date2 = datetime2.date()
        df0 = pandas.DataFrame(
            data={
                'a': [date1, date2],
                'b': [datetime1, datetime2],
                'c': [True, False],
                'd': [4, 3],
                'e': [1.1, 2.2],
                'f': ['1', '2'],
                'g': [5, 7],
                'h': [True, False],
                'i': [3, 9],
                'j': ['10', '3'],
                'k': [3, 10],
                'l': [pandas.NA, True],
                'm': [5, pandas.NA],
                'n': [pandas.NA, 8.8],
                'o': [pandas.NA, '1'],
                'p': [datetime1, pandas.NA],
                'q': [pandas.NA, date2]
            }
        )

        df0['g'] = df0['g'].astype(numpy.unsignedinteger)
        df0['h'] = df0['h'].astype(pandas.BooleanDtype())
        df0['i'] = df0['i'].astype(pandas.UInt32Dtype())
        df0['j'] = df0['j'].astype(pandas.StringDtype())
        df0['k'] = df0['k'].astype(pandas.CategoricalDtype())
        df0['l'] = df0['l'].astype(pandas.BooleanDtype())

        loaders.gpl11.load(
            source='dataframe',
            destination='bq',
            data_name='a100',
            dataframe=df0,
            date_cols=['a', 'q'],
            timestamp_cols=['b', 'p'])
        table_id = ids.build_table_id('a100')
        table = bq_client.get_table(table_id)
        f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, \
            f16, f17 = table.schema
        self.assertEqual(('a', 'DATE'), (f1.name, f1.field_type))
        self.assertEqual(('b', 'TIMESTAMP'), (f2.name, f2.field_type))
        self.assertEqual(('c', 'BOOLEAN'), (f3.name, f3.field_type))
        self.assertEqual(('d', 'INTEGER'), (f4.name, f4.field_type))
        self.assertEqual(('e', 'FLOAT'), (f5.name, f5.field_type))
        self.assertEqual(('f', 'STRING'), (f6.name, f6.field_type))
        self.assertEqual(('g', 'INTEGER'), (f7.name, f7.field_type))
        self.assertEqual(('h', 'BOOLEAN'), (f8.name, f8.field_type))
        self.assertEqual(('i', 'INTEGER'), (f9.name, f9.field_type))
        self.assertEqual(('j', 'STRING'), (f10.name, f10.field_type))
        self.assertEqual(('k', 'STRING'), (f11.name, f11.field_type))
        self.assertEqual(('l', 'BOOLEAN'), (f12.name, f12.field_type))
        self.assertEqual(('m', 'INTEGER'), (f13.name, f13.field_type))
        self.assertEqual(('n', 'FLOAT'), (f14.name, f14.field_type))
        self.assertEqual(('o', 'STRING'), (f15.name, f15.field_type))
        self.assertEqual(('p', 'TIMESTAMP'), (f16.name, f16.field_type))
        self.assertEqual(('q', 'DATE'), (f17.name, f17.field_type))

    def test_bq_schema_inferred_with_source_local(self):
        datetime1 = datetime.strptime('2012-11-14 14:32:30',
                                      '%Y-%m-%d %H:%M:%S')
        datetime2 = datetime.strptime('2003-11-14 14:32:30.100121',
                                      '%Y-%m-%d %H:%M:%S.%f')
        date1 = datetime1.date()
        date2 = datetime2.date()
        df0 = pandas.DataFrame(
            data={
                'a': [date1, date2],
                'b': [datetime1, datetime2],
                'c': [True, False],
                'd': [4, 3],
                'e': [1.1, 2.2],
                'f': ['1', '2'],
                'g': [5, 7],
                'h': [True, False],
                'i': [3, 9],
                'j': ['10', '3'],
                'k': [3, 10],
                'l': [pandas.NA, True],
                'm': [5, pandas.NA],
                'n': [pandas.NA, 8.8],
                'o': [pandas.NA, '1'],
                'p': [datetime1, pandas.NA],
                'q': [pandas.NA, date2]
            }
        )

        load.dataframe_to_local(df0, ids.build_local_file_path_1('a100'))
        loaders.gpl01.load(
            source='local',
            destination='bq',
            data_name='a100')
        table_id = ids.build_table_id('a100')
        table = bq_client.get_table(table_id)
        f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, \
            f16, f17 = table.schema
        self.assertEqual(('a', 'DATE'), (f1.name, f1.field_type))
        self.assertEqual(('b', 'TIMESTAMP'), (f2.name, f2.field_type))
        self.assertEqual(('c', 'BOOLEAN'), (f3.name, f3.field_type))
        self.assertEqual(('d', 'INTEGER'), (f4.name, f4.field_type))
        self.assertEqual(('e', 'FLOAT'), (f5.name, f5.field_type))
        self.assertEqual(('f', 'INTEGER'), (f6.name, f6.field_type))
        self.assertEqual(('g', 'INTEGER'), (f7.name, f7.field_type))
        self.assertEqual(('h', 'BOOLEAN'), (f8.name, f8.field_type))
        self.assertEqual(('i', 'INTEGER'), (f9.name, f9.field_type))
        self.assertEqual(('j', 'INTEGER'), (f10.name, f10.field_type))
        self.assertEqual(('k', 'INTEGER'), (f11.name, f11.field_type))
        self.assertEqual(('l', 'BOOLEAN'), (f12.name, f12.field_type))
        self.assertEqual(('m', 'INTEGER'), (f13.name, f13.field_type))
        self.assertEqual(('n', 'FLOAT'), (f14.name, f14.field_type))
        self.assertEqual(('o', 'INTEGER'), (f15.name, f15.field_type))
        self.assertEqual(('p', 'TIMESTAMP'), (f16.name, f16.field_type))
        self.assertEqual(('q', 'DATE'), (f17.name, f17.field_type))

    def test_bq_schema_given_with_source_dataframe(self):
        df0 = pandas.DataFrame(data={'x': ['1'], 'y': [3]})
        bq_schema = [bigquery.SchemaField(name='x', field_type='FLOAT'),
                     bigquery.SchemaField(name='y', field_type='FLOAT')]
        loaders.gpl21.load(
            source='dataframe',
            destination='bq',
            data_name='a100',
            dataframe=df0,
            bq_schema=bq_schema)
        table_id = ids.build_table_id('a100')
        table = bq_client.get_table(table_id)
        f1, f2 = table.schema
        self.assertEqual(('x', 'FLOAT'), (f1.name, f1.field_type))
        self.assertEqual(('y', 'FLOAT'), (f2.name, f2.field_type))

    def test_bq_schema_given_with_source_gs(self):
        df0 = pandas.DataFrame(data={'x': ['1'], 'y': [3]})
        bq_schema = [bigquery.SchemaField(name='x', field_type='FLOAT'),
                     bigquery.SchemaField(name='y', field_type='FLOAT')]
        load.dataframe_to_gs(df0, ids.build_blob_name_2('a100'))
        loaders.gpl21.load(
            source='gs',
            destination='bq',
            data_name='a100',
            bq_schema=bq_schema)
        table_id = ids.build_table_id('a100')
        table = bq_client.get_table(table_id)
        f1, f2 = table.schema
        self.assertEqual(('x', 'FLOAT'), (f1.name, f1.field_type))
        self.assertEqual(('y', 'FLOAT'), (f2.name, f2.field_type))
