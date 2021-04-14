import pandas
from google_pandas_load.constants import ATOMIC_FUNCTION_NAMES
from google_pandas_load import LoadConfig
from tests.context.loaders import gpl3
from tests.base_class import BaseClassTest


class ExtraInformationsTest(BaseClassTest):

    def test_xload(self):
        xlr = gpl3.xload(source='query', destination='dataframe',
                         query='select 3')

        self.assertEqual(set(vars(xlr)),
                         {'load_result',
                          'data_name',
                          'duration',
                          'durations',
                          'query_cost'})

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
            destination='bq',
            data_name='e100',
            dataframe=df0)
        config2 = LoadConfig(
            source='query',
            destination='bq',
            data_name='e101',
            query='select 3')
        config3 = LoadConfig(
            source='dataframe',
            destination='bq',
            data_name='e102',
            dataframe=df0)
        xmlr = gpl3.xmload([config1, config2, config3])
        self.assertEqual(
            set(vars(xmlr)),
            {'load_results', 'data_names', 'duration', 'durations',
             'query_cost', 'query_costs'})

        self.assertEqual(xmlr.data_names, ['e100', 'e101', 'e102'])

        self.assertTrue(xmlr.duration > 0)

        self.assertEqual(set(vars(xmlr.durations)), set(ATOMIC_FUNCTION_NAMES))

        for n in ATOMIC_FUNCTION_NAMES:
            duration = vars(xmlr.durations)[n]
            if duration is not None:
                self.assertTrue(duration >= 0)

        self.assertEqual(xmlr.query_cost, 0.0)

        self.assertEqual(xmlr.query_costs, [None, 0.0, None])
