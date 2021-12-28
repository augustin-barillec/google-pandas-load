from tests.populate import populate_bq, populate_gs, populate_local
from tests.base_class import BaseClassTest
from tests import loaders


class ExistMethodsTest(BaseClassTest):

    def test_exist_in_bq(self):
        self.assertFalse(loaders.gpl01.exist_in_bq('a8_bq'))
        populate_bq()
        self.assertTrue(loaders.gpl01.exist_in_bq('a8_bq'))

    def test_exist_in_gs(self):
        self.assertFalse(loaders.gpl01.exist_in_gs('a1'))
        self.assertFalse(loaders.gpl11.exist_in_gs('a10'))
        self.assertFalse(loaders.gpl21.exist_in_gs('a'))
        populate_gs()
        self.assertTrue(loaders.gpl01.exist_in_gs('a1'))
        self.assertTrue(loaders.gpl11.exist_in_gs('a10'))
        self.assertTrue(loaders.gpl21.exist_in_gs('a'))

    def test_exist_in_local(self):
        self.assertFalse(loaders.gpl00.exist_in_local('a'))
        self.assertFalse(loaders.gpl01.exist_in_local('a9'))
        populate_local()
        self.assertTrue(loaders.gpl00.exist_in_local('a'))
        self.assertTrue(loaders.gpl01.exist_in_local('a9'))
