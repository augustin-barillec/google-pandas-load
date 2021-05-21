import re
import logging
from google_pandas_load.loader import logger_ as loader_default_logger
from google_pandas_load.loader_quick_setup import formatter \
    as loader_quicksetup_default_logger_formatter
from tests.context.loaders import gpl1, gpl3, gpl4, gpl5
from tests.base_class import BaseClassTest
from tests.populate import populate_local_folder

logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
ch = logging.StreamHandler()
fmt = '%(name)s # %(levelname)s # %(message)s'
formatter = logging.Formatter(fmt=fmt)
ch.setFormatter(fmt=formatter)
logger.addHandler(hdlr=ch)


class LoggingTest(BaseClassTest):

    def test_default_loader_logger(self):

        populate_local_folder()

        with self.assertLogs('Loader', level='INFO') as cm:
            gpl3.load(
                source='local',
                destination='gs',
                data_name='a9')
            records = cm.records
            self.assertEqual(len(records), 2)
            log = formatter.format(records[0])
            self.assertEqual(
                log,
                'Loader # INFO # Starting local to gs...')

    def test_default_loader_quicksetup_logger(self):

        populate_local_folder()

        with self.assertLogs('LoaderQuickSetup', level='DEBUG') as cm:
            gpl5.load(
                source='local',
                destination='dataframe',
                data_name='a9')
            records = cm.records
            self.assertEqual(len(records), 2)
            regexp = (r'- LoaderQuickSetup - DEBUG - '
                      r'Ended local to dataframe \[[0-9]+s\]$')
            pattern = re.compile(regexp)
            log = loader_quicksetup_default_logger_formatter.format(records[1])
            self.assertIsNotNone(pattern.search(log))

    def test_custom_logger(self):

        with self.assertLogs('tests.context.loaders', level='INFO') as cm:
            gpl4.load(
                source='query',
                destination='gs',
                data_name='a0',
                query='select 3')
            records = cm.records
            self.assertEqual(len(records), 4)
            regexp = (r'^tests.context.loaders # WARNING # '
                      r'Ended query to bq \[[0-9]+s, [0-9]\.[0-9]+\$\]$')
            pattern = re.compile(regexp)
            log = formatter.format(records[1])
            self.assertIsNotNone(pattern.search(log))

    def test_log_level(self):

        with self.assertLogs('Loader', level='DEBUG') as cm:
            gpl1.load(
                source='query',
                destination='bq',
                data_name='a0',
                query='select 3')
            records = cm.records
            self.assertEqual(len(records), 2)

        with self.assertLogs('Loader', level='INFO') as cm:
            gpl1.load(
                source='query',
                destination='bq',
                data_name='a0',
                query='select 3')
            loader_default_logger.info('Dummy info log')
            records = cm.records
            self.assertEqual(len(records), 1)
            self.assertEqual(cm.records[0].msg, 'Dummy info log')
