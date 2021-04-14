from tests.context.loaders import gpl1, gpl2, gpl4, gpl5
from tests.base_class import BaseClassTest
from tests.populate import populate_dataset, populate_bucket, \
    populate_local_folder


class DeleteMethodsTest(BaseClassTest):

    def test_delete_in_bq(self):
        populate_dataset()
        self.assertTrue(gpl4.exist_in_bq('a11_bq'))
        gpl4.delete_in_bq('a11_bq')
        self.assertFalse(gpl4.exist_in_bq('a11_bq'))

    def test_delete_in_gs(self):
        populate_bucket()

        self.assertTrue(gpl1.exist_in_gs('a'))
        gpl1.delete_in_gs('a')
        self.assertFalse(gpl1.exist_in_gs('a'))

        self.assertTrue(gpl2.exist_in_gs('a1'))
        gpl2.delete_in_gs('a1')
        self.assertFalse(gpl2.exist_in_gs('a1'))

    def test_delete_in_local(self):
        populate_local_folder()

        self.assertTrue(gpl5.exist_in_local('a12'))
        gpl5.delete_in_local('a12')
        self.assertFalse(gpl5.exist_in_local('a12'))
