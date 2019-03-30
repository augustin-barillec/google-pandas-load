from tests.context.loaders import *
from tests.utils import *


class DeleteMethodsTest(BaseClassTest):

    def test_delete_in_bq(self):
        self.assertTrue(gpl4.exist_in_bq(data_name='a0'))
        gpl4.delete_in_bq(data_name='a0')
        self.assertFalse(gpl4.exist_in_bq(data_name='a0'))

    def test_delete_in_gs(self):
        self.assertTrue(gpl1.exist_in_gs(data_name='a0'))
        gpl1.delete_in_gs(data_name='a0')
        self.assertFalse(gpl1.exist_in_gs(data_name='a0'))

        self.assertTrue(gpl2.exist_in_gs(data_name='a0'))
        gpl2.delete_in_gs(data_name='a0')
        self.assertFalse(gpl2.exist_in_gs(data_name='a0'))

    def test_delete_in_local(self):
        self.assertTrue(gpl5.exist_in_local(data_name='a0'))
        gpl5.delete_in_local(data_name='a0')
        self.assertFalse(gpl5.exist_in_local(data_name='a0'))
