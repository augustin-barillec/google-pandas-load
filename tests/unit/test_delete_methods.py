from tests.context.loaders import *
from tests.utils import *


class DeleteMethodsTest(BaseClassTest):

    def test_delete_in_bq(self):
        populate_dataset()
        self.assertTrue(gpl4.exist_in_bq(data_name='a11_bq'))
        gpl4.delete_in_bq(data_name='a11_bq')
        self.assertFalse(gpl4.exist_in_bq(data_name='a11_bq'))

    def test_delete_in_gs(self):
        populate_bucket()

        self.assertTrue(gpl1.exist_in_gs(data_name='a'))
        gpl1.delete_in_gs(data_name='a')
        self.assertFalse(gpl1.exist_in_gs(data_name='a'))

        self.assertTrue(gpl2.exist_in_gs(data_name='a1'))
        gpl2.delete_in_gs(data_name='a1')
        self.assertFalse(gpl2.exist_in_gs(data_name='a1'))

    def test_delete_in_local(self):
        populate_local_folder()

        self.assertTrue(gpl5.exist_in_local(data_name='a12'))
        gpl5.delete_in_local(data_name='a12')
        self.assertFalse(gpl5.exist_in_local(data_name='a12'))
