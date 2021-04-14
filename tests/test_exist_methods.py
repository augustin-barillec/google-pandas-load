from tests.context.loaders import gpl1, gpl2, gpl4, gpl5
from tests.base_class import BaseClassTest
from tests.populate import populate_dataset, populate_bucket, \
    populate_local_folder


class ExistMethodsTest(BaseClassTest):

    def test_exist_in_bq(self):
        self.assertFalse(gpl4.exist_in_bq('a8_bq'))
        populate_dataset()
        self.assertTrue(gpl4.exist_in_bq('a8_bq'))

    def test_exist_in_gs(self):
        self.assertFalse(gpl1.exist_in_gs('a1'))
        self.assertFalse(gpl2.exist_in_gs('a10'))
        populate_bucket()
        self.assertTrue(gpl1.exist_in_gs('a1'))
        self.assertTrue(gpl2.exist_in_gs('a10'))

    def test_exist_in_local(self):
        self.assertFalse(gpl5.exist_in_local('a'))
        populate_local_folder()
        self.assertTrue(gpl5.exist_in_local('a'))
