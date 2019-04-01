from tests.context.loaders import *
from tests.utils import *


class LoadParametersTest(BaseClassTest):

    def test_wildcard(self):
        gpl1.load(source='query', destination='gs', data_name='e0', query='select 2')
        list_blob_uris = gpl1.list_blob_uris(data_name='e0')
        self.assertEqual(list_blob_uris, ['gs://{}/e0-000000000000.csv.gz'.format(bucket_name)])

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
        table = bq_client.get_table(table_ref=table_ref)
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
        table = bq_client.get_table(table_ref=table_ref)
        num_rows = table.num_rows
        self.assertEqual(num_rows, 2)


