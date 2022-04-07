import re
import logging
from tests.utils.populate import populate_local
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


fmt = '%(name)s # %(levelname)s # %(message)s'
formatter = logging.Formatter(fmt=fmt)


class LoggingTest(BaseClassTest):

    def test_local_to_bucket(self):
        populate_local()
        with self.assertLogs('google_pandas_load.loader', level='DEBUG') as cm:
            loaders.gpl10.load(
                source='local',
                destination='bucket',
                data_name='a9')
            records = cm.records
            self.assertEqual(2, len(records))
            log = formatter.format(records[0])
            self.assertEqual(
                'google_pandas_load.loader # DEBUG # '
                'Starting local to bucket...',
                log)

    def test_local_to_dataframe(self):
        populate_local()
        with self.assertLogs('google_pandas_load.loader', level='DEBUG') as cm:
            loaders.gpl20.load(
                source='local',
                destination='dataframe',
                data_name='a9')
            records = cm.records
            self.assertEqual(2, len(records))
            regexp = (r'^google_pandas_load.loader # DEBUG # '
                      r'Ended local to dataframe \[[0-9]+s\]$')
            pattern = re.compile(regexp)
            log = formatter.format(records[1])
            self.assertIsNotNone(pattern.search(log))

    def test_query_to_bucket(self):
        with self.assertLogs('google_pandas_load.loader', level='DEBUG') as cm:
            loaders.gpl11.load(
                source='query',
                destination='bucket',
                query='select 3',
                data_name='a0')
            records = cm.records
            self.assertEqual(4, len(records))
            regexp = (r'^google_pandas_load.loader # DEBUG # '
                      r'Ended query to dataset \[[0-9]+s, [0-9]+\.[0-9]+\$\]$')
            pattern = re.compile(regexp)
            log = formatter.format(records[1])
            self.assertIsNotNone(pattern.search(log))
