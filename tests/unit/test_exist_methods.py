from tests.context.loaders import *
from tests.utils import *


class ExistMethodsTest(BaseClassTest):

    def test_exist_in_bq(self):
        self.assertFalse(gpl4.exist_in_bq(data_name='a0'))
        populate_bq()
        self.assertTrue(gpl4.exist_in_bq(data_name='a0'))

    def test_exist_in_gs(self):
        self.assertFalse(gpl1.exist_in_gs(data_name='a0'))
        self.assertFalse(gpl2.exist_in_gs(data_name='a0'))
        populate_gs()
        self.assertTrue(gpl1.exist_in_gs(data_name='a0'))
        self.assertTrue(gpl2.exist_in_gs(data_name='a0'))

    def test_exist_in_local(self):
        self.assertFalse(gpl5.exist_in_local(data_name='a0'))
        populate_local()
        self.assertTrue(gpl5.exist_in_local(data_name='a0'))
