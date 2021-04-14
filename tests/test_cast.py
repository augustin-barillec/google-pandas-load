import pytz
import numpy
import pandas
from datetime import datetime
from google.cloud import bigquery
from tests.context.resources import dataset_ref, bq_client
from tests.context.loaders import gpl2, gpl3, gpl4
from tests.base_class import BaseClassTest
from tests.populate import populate


class CastTest(BaseClassTest):

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
        populate()

        datetime1 = datetime(2012, 11, 14, 14, 32, 30, tzinfo=pytz.UTC)
        datetime2 = datetime(2013, 11, 14, 14, 32, 30, 100121)

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
            parse_dates=['x', 'y', 'z'])
        df1['z'] = df1['z'].apply(lambda z: z.date())
        self.assertTrue(df0.equals(df1))

    def test_bq_schema_inferred_from_dataframe(self):
        populate()
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
                'o': [pandas.NA, '1']
            }
        )

        df0['g'] = df0['g'].astype(numpy.unsignedinteger)
        df0['h'] = df0['h'].astype(pandas.BooleanDtype())
        df0['i'] = df0['i'].astype(pandas.UInt32Dtype())
        df0['j'] = df0['j'].astype(pandas.StringDtype())
        df0['k'] = df0['k'].astype(pandas.CategoricalDtype())
        df0['l'] = df0['l'].astype(pandas.BooleanDtype())

        gpl3.load(
            source='dataframe',
            destination='bq',
            data_name='a10',
            dataframe=df0,
            date_cols=['a'],
            timestamp_cols=['b'])
        table_ref = dataset_ref.table(table_id='a10')
        table = bq_client.get_table(table_ref)
        f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15 = \
            table.schema
        self.assertEqual((f1.name, f1.field_type), ('a', 'DATE'))
        self.assertEqual((f2.name, f2.field_type), ('b', 'TIMESTAMP'))
        self.assertEqual((f3.name, f3.field_type), ('c', 'BOOLEAN'))
        self.assertEqual((f4.name, f4.field_type), ('d', 'INTEGER'))
        self.assertEqual((f5.name, f5.field_type), ('e', 'FLOAT'))
        self.assertEqual((f6.name, f6.field_type), ('f', 'STRING'))
        self.assertEqual((f7.name, f7.field_type), ('g', 'INTEGER'))
        self.assertEqual((f8.name, f8.field_type), ('h', 'BOOLEAN'))
        self.assertEqual((f9.name, f9.field_type), ('i', 'INTEGER'))
        self.assertEqual((f10.name, f10.field_type), ('j', 'STRING'))
        self.assertEqual((f11.name, f11.field_type), ('k', 'STRING'))
        self.assertEqual((f12.name, f12.field_type), ('l', 'BOOLEAN'))
        self.assertEqual((f13.name, f13.field_type), ('m', 'INTEGER'))
        self.assertEqual((f14.name, f14.field_type), ('n', 'FLOAT'))
        self.assertEqual((f15.name, f15.field_type), ('o', 'STRING'))

    def test_bq_schema_inferred_from_csv(self):
        populate()
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
                'o': [pandas.NA, '1']
            }
        )

        gpl3.load(
            source='dataframe',
            destination='local',
            data_name='a10',
            dataframe=df0)

        gpl3.load(
            source='local',
            destination='bq',
            data_name='a10')
        table_ref = dataset_ref.table(table_id='a10')
        table = bq_client.get_table(table_ref)
        f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15 = \
            table.schema
        self.assertEqual((f1.name, f1.field_type), ('a', 'DATE'))
        self.assertEqual((f2.name, f2.field_type), ('b', 'TIMESTAMP'))
        self.assertEqual((f3.name, f3.field_type), ('c', 'BOOLEAN'))
        self.assertEqual((f4.name, f4.field_type), ('d', 'INTEGER'))
        self.assertEqual((f5.name, f5.field_type), ('e', 'FLOAT'))
        self.assertEqual((f6.name, f6.field_type), ('f', 'INTEGER'))
        self.assertEqual((f7.name, f7.field_type), ('g', 'INTEGER'))
        self.assertEqual((f8.name, f8.field_type), ('h', 'BOOLEAN'))
        self.assertEqual((f9.name, f9.field_type), ('i', 'INTEGER'))
        self.assertEqual((f10.name, f10.field_type), ('j', 'INTEGER'))
        self.assertEqual((f11.name, f11.field_type), ('k', 'INTEGER'))
        self.assertEqual((f12.name, f12.field_type), ('l', 'BOOLEAN'))
        self.assertEqual((f13.name, f13.field_type), ('m', 'INTEGER'))
        self.assertEqual((f14.name, f14.field_type), ('n', 'FLOAT'))
        self.assertEqual((f15.name, f15.field_type), ('o', 'INTEGER'))

    def test_bq_schema_given(self):
        populate()
        df0 = pandas.DataFrame(data={'x': ['1'], 'y': [3]})
        bq_schema = [bigquery.SchemaField(name='x', field_type='FLOAT'),
                     bigquery.SchemaField(name='y', field_type='FLOAT')]
        gpl4.load(
            source='dataframe',
            destination='bq',
            data_name='a',
            dataframe=df0,
            bq_schema=bq_schema)
        table_ref = dataset_ref.table(table_id='a')
        table = bq_client.get_table(table_ref)
        f1, f2 = table.schema
        self.assertEqual((f1.name, f1.field_type), ('x', 'FLOAT'))
        self.assertEqual((f2.name, f2.field_type), ('y', 'FLOAT'))
