from tests.utils.populate import \
    populate_dataset, populate_bucket, populate_local
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


class ExistTest(BaseClassTest):

    def test_exist_in_dataset(self):
        self.assertFalse(loaders.gpl01.exist_in_dataset('a8'))
        populate_dataset()
        self.assertTrue(loaders.gpl01.exist_in_dataset('a8'))

    def test_exist_in_bucket(self):
        self.assertFalse(loaders.gpl01.exist_in_bucket('a1'))
        self.assertFalse(loaders.gpl11.exist_in_bucket('a10'))
        self.assertFalse(loaders.gpl21.exist_in_bucket('a'))
        populate_bucket()
        self.assertTrue(loaders.gpl01.exist_in_bucket('a1'))
        self.assertTrue(loaders.gpl11.exist_in_bucket('a10'))
        self.assertTrue(loaders.gpl21.exist_in_bucket('a'))

    def test_exist_in_local(self):
        self.assertFalse(loaders.gpl00.exist_in_local('a'))
        self.assertFalse(loaders.gpl01.exist_in_local('a9'))
        populate_local()
        self.assertTrue(loaders.gpl00.exist_in_local('a'))
        self.assertTrue(loaders.gpl01.exist_in_local('a9'))
