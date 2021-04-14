import pandas
from tests.context.loaders import gpl2
from tests.context.resources import bq_client, dataset_ref
from tests.base_class import BaseClassTest
from tests.populate import populate_dataset, populate


class LoadParametersTest(BaseClassTest):

    def test_write_append_dataframe_to_bq(self):
        populate()
        df0 = pandas.DataFrame(data={'x': [1]})
        gpl2.load(
            source='dataframe',
            destination='bq',
            data_name='a10',
            dataframe=df0)
        gpl2.load(
            source='dataframe',
            destination='bq',
            data_name='a10',
            dataframe=df0,
            write_disposition='WRITE_APPEND')
        table_ref = dataset_ref.table(table_id='a10')
        table = bq_client.get_table(table_ref)
        num_rows = table.num_rows
        self.assertEqual(num_rows, 2)

    def test_write_append_query_to_bq(self):
        populate_dataset()
        gpl2.load(
            source='query',
            destination='bq',
            data_name='a10',
            query='select 3 as x')
        gpl2.load(
            source='query',
            destination='bq',
            data_name='a10',
            query='select 4 as x',
            write_disposition='WRITE_APPEND')
        table_ref = dataset_ref.table(table_id='a10')
        table = bq_client.get_table(table_ref)
        num_rows = table.num_rows
        self.assertEqual(num_rows, 2)
