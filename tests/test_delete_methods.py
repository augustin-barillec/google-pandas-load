from tests.populate import populate_bq, populate_gs, populate_local
from tests.base_class import BaseClassTest
from tests import loaders


class DeleteMethodsTest(BaseClassTest):

    def test_delete_in_bq(self):
        populate_bq()
        self.assertTrue(loaders.gpl00.exist_in_bq('a11_bq'))
        loaders.gpl00.delete_in_bq('a11_bq')
        self.assertFalse(loaders.gpl00.exist_in_bq('a11_bq'))

    def test_delete_in_gs(self):
        populate_gs()

        self.assertTrue(loaders.gpl00.exist_in_gs('a'))
        loaders.gpl00.delete_in_gs('a')
        self.assertFalse(loaders.gpl00.exist_in_gs('a'))

        self.assertTrue(loaders.gpl10.exist_in_gs('a1'))
        loaders.gpl10.delete_in_gs('a1')
        self.assertFalse(loaders.gpl10.exist_in_gs('a1'))

        self.assertTrue(loaders.gpl20.exist_in_gs('a'))
        loaders.gpl20.delete_in_gs('a')
        self.assertFalse(loaders.gpl20.exist_in_gs('a'))

    def test_delete_in_local(self):
        populate_local()

        self.assertTrue(loaders.gpl20.exist_in_local('a12'))
        loaders.gpl20.delete_in_local('a12')
        self.assertFalse(loaders.gpl20.exist_in_local('a12'))

        self.assertTrue(loaders.gpl01.exist_in_local('a'))
        loaders.gpl01.delete_in_local('a')
        self.assertFalse(loaders.gpl01.exist_in_local('a'))
