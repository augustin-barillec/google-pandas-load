from tests.context.loaders import *
from tests.utils import *


class DisplayLogTest(BaseClassTest):

    def test_display_warning_log(self):
        with self.assertLogs('Loader', level='WARNING') as cm:
            gpl1.delete_in_bq(data_name='a10')
        self.assertEqual(cm.output, ['WARNING:Loader:There is no data named a10 in bq'])

        with self.assertLogs('Loader', level='WARNING') as cm:
            gpl1.delete_in_gs(data_name='a10')
        self.assertEqual(cm.output, ['WARNING:Loader:There is no data named a10 in gs'])

        with self.assertLogs('Loader', level='WARNING') as cm:
            gpl1.delete_in_local(data_name='a10')
        self.assertEqual(cm.output, ['WARNING:Loader:There is no data named a10 in local'])
