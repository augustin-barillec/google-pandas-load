from tests.context.loaders import *
from tests.utils import *


class DataDeliveryTest(BaseClassTest):

    def test_query_to_bq(self):
        populate_dataset()
        self.assertFalse(gpl3.exist_in_bq('a0'))
        gpl3.load(
            source='query',
            destination='bq',
            data_name='a0',
            query='select 3 as x')
        self.assertTrue(gpl3.exist_in_bq('a0'))

    def test_bq_to_dataframe(self):
        df0 = pandas.DataFrame(data={'x': ['data_a10_bq']})
        populate()
        df1 = gpl4.load(
            source='bq',
            destination='dataframe',
            data_name='a10_bq')
        self.assertFalse(gpl4.exist_in_bq(data_name='a10_bq'))
        self.assertTrue(df0.equals(df1))

    def test_gs_to_local(self):
        populate_bucket()
        gpl2.load(
            source='gs',
            destination='local',
            data_name='a7',
            delete_in_gs=False)
        self.assertEqual(len(gpl2.list_blob_uris(data_name='a7')), 1)
        self.assertEqual(len(gpl2.list_local_file_paths(data_name='a7')), 1)

    def test_local_to_dataframe(self):
        df0 = pandas.DataFrame(data={'x': ['data_a{}_local'.format(i) for i in range(10, 14)]})
        populate_local_folder()
        df1 = gpl5.load(
            source='local',
            destination='dataframe',
            data_name='a1')
        self.assertEqual(sorted(list(df0['x'])), sorted(list(df1['x'])))

    def query_to_dataframe(self):
        df0 = pandas.DataFrame(data={'x': [1, 1]})
        populate()
        df1 = gpl2.load(
            source='query',
            destination='dataframe',
            query='select 1 as x union all select 1 as x',
            data_name='a1',
            delete_in_bq=False)
        self.assertTrue(df0.equals(df1))









