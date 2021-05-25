import re
import logging
from tests.context.loaders import gpl3, gpl4, gpl5
from tests.base_class import BaseClassTest
from tests.populate import populate_local_folder

fmt = '%(name)s # %(levelname)s # %(message)s'
formatter = logging.Formatter(fmt=fmt)


class LoggingTest(BaseClassTest):

    def test_local_to_gs(self):
        populate_local_folder()
        with self.assertLogs('google_pandas_load.loader', level='DEBUG') as cm:
            gpl3.load(
                source='local',
                destination='gs',
                data_name='a9')
            records = cm.records
            self.assertEqual(len(records), 2)
            log = formatter.format(records[0])
            self.assertEqual(
                log,
                'google_pandas_load.loader # DEBUG # Starting local to gs...')

    def test_local_to_dataframe(self):
        populate_local_folder()
        with self.assertLogs('google_pandas_load.loader', level='DEBUG') as cm:
            gpl5.load(
                source='local',
                destination='dataframe',
                data_name='a9')
            records = cm.records
            self.assertEqual(len(records), 2)
            regexp = (r'^google_pandas_load.loader # DEBUG # '
                      r'Ended local to dataframe \[[0-9]+s\]$')
            pattern = re.compile(regexp)
            log = formatter.format(records[1])
            self.assertIsNotNone(pattern.search(log))

    def test_query_to_gs(self):
        with self.assertLogs('google_pandas_load.loader', level='DEBUG') as cm:
            gpl4.load(
                source='query',
                destination='gs',
                data_name='a0',
                query='select 3')
            records = cm.records
            self.assertEqual(len(records), 4)
            regexp = (r'^google_pandas_load.loader # DEBUG # '
                      r'Ended query to bq \[[0-9]+s, [0-9]\.[0-9]+\$\]$')
            pattern = re.compile(regexp)
            log = formatter.format(records[1])
            self.assertIsNotNone(pattern.search(log))
