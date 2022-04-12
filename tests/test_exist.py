from tests.utils import constants
from tests.utils.populate import \
    populate_dataset, populate_bucket, populate_local
from tests.utils.loader import create_loader
from tests.utils.base_class import BaseClassTest


class ExistTest(BaseClassTest):

    def test_exist_in_dataset(self):
        gpl = create_loader(local_dir_path=constants.local_subdir_path)
        self.assertFalse(gpl.exist_in_dataset('a8'))
        populate_dataset()
        self.assertTrue(gpl.exist_in_dataset('a8'))

    def test_exist_in_bucket(self):
        gpl01 = create_loader(local_dir_path=constants.local_subdir_path)
        gpl11 = create_loader(
            bucket_dir_path=constants.bucket_dir_path,
            local_dir_path=constants.local_subdir_path)
        gpl21 = create_loader(
            bucket_dir_path=constants.bucket_subdir_path,
            local_dir_path=constants.local_subdir_path)
        self.assertFalse(gpl01.exist_in_bucket('a1'))
        self.assertFalse(gpl11.exist_in_bucket('a10'))
        self.assertFalse(gpl21.exist_in_bucket('a'))
        populate_bucket()
        self.assertTrue(gpl01.exist_in_bucket('a1'))
        self.assertTrue(gpl11.exist_in_bucket('a10'))
        self.assertTrue(gpl21.exist_in_bucket('a'))

    def test_exist_in_local(self):
        gpl00 = create_loader()
        gpl01 = create_loader(local_dir_path=constants.local_subdir_path)
        self.assertFalse(gpl00.exist_in_local('a'))
        self.assertFalse(gpl01.exist_in_local('a9'))
        populate_local()
        self.assertTrue(gpl00.exist_in_local('a'))
        self.assertTrue(gpl01.exist_in_local('a9'))
