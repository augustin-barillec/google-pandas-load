import re
import pandas
from google_pandas_load.constants import ATOMIC_FUNCTION_NAMES
from google_pandas_load import LoadConfig
from tests.base_class import BaseClassTest
from tests import loaders


class ExtraInformationsTest(BaseClassTest):

    def test_xload(self):
        xlr = loaders.gpl20.xload(
            source='query', destination='dataframe', query='select 3')

        self.assertEqual(
            {'load_result', 'data_name', 'duration',
             'durations', 'query_cost'},
            set(vars(xlr)))

        self.assertEqual(str, type(xlr.data_name))
        regexp = r'^[0-9]{14}_[0-9]{6}_rand[0-9]{39}$'
        pattern = re.compile(regexp)
        self.assertIsNotNone(pattern.search(xlr.data_name))

        self.assertTrue(xlr.duration > 0)

        self.assertEqual(
            sorted(ATOMIC_FUNCTION_NAMES),
            sorted(vars(xlr.durations)))

        for n in ATOMIC_FUNCTION_NAMES:
            duration = vars(xlr.durations)[n]
            if duration is not None:
                self.assertTrue(duration >= 0)

        self.assertEqual(0.0, xlr.query_cost)

    def test_xmload(self):
        df0 = pandas.DataFrame(data={'x': [4]})
        config1 = LoadConfig(
            source='dataframe',
            destination='bq',
            dataframe=df0,
            data_name='e100')
        config2 = LoadConfig(
            source='dataframe',
            destination='bq',
            dataframe=df0,
            data_name='e101')
        config3 = LoadConfig(
            source='query',
            destination='dataframe',
            query='select 3',
            data_name='e102')
        xmlr = loaders.gpl21.xmload([config1, config2, config3])
        self.assertEqual(
            {'load_results', 'data_names', 'duration', 'durations',
             'query_cost', 'query_costs'},
            set(vars(xmlr)))

        self.assertEqual(['e100', 'e101', 'e102'], xmlr.data_names)

        self.assertTrue(xmlr.duration > 0)

        self.assertEqual(
            sorted(ATOMIC_FUNCTION_NAMES),
            sorted(vars(xmlr.durations)))

        for n in ATOMIC_FUNCTION_NAMES:
            duration = vars(xmlr.durations)[n]
            if duration is not None:
                self.assertTrue(duration >= 0)

        self.assertEqual(0.0, xmlr.query_cost)

        self.assertEqual([None, None, 0.0], xmlr.query_costs)
