from tests.utils import ids
from tests.utils import exist
from tests.utils.populate import populate_bq, populate_gs, populate_local
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


class DeleteMethodsTest(BaseClassTest):

    def test_delete_in_bq(self):
        populate_bq()
        self.assertTrue(exist.table_exists('a11'))
        loaders.gpl00.delete_in_bq('a11')
        self.assertFalse(exist.table_exists('a11'))

    def test_delete_in_gs(self):
        populate_gs()

        blob_name = ids.build_blob_name_0('a7')
        self.assertTrue(exist.blob_exists(blob_name))
        loaders.gpl00.delete_in_gs('a7')
        self.assertFalse(exist.blob_exists(blob_name))

        blob_name = ids.build_blob_name_1('a10')
        self.assertTrue(exist.blob_exists(blob_name))
        loaders.gpl10.delete_in_gs('a1')
        self.assertFalse(exist.blob_exists(blob_name))

        blob_name = ids.build_blob_name_2('a13')
        self.assertTrue(exist.blob_exists(blob_name))
        loaders.gpl20.delete_in_gs('a')
        self.assertFalse(exist.blob_exists(blob_name))

    def test_delete_in_local(self):
        populate_local()

        local_file_path = ids.build_local_file_path_0('a11')
        self.assertTrue(exist.local_file_exists(local_file_path))
        loaders.gpl20.delete_in_local('a11')
        self.assertFalse(exist.local_file_exists(local_file_path))

        local_file_path = ids.build_local_file_path_1('a12')
        self.assertTrue(exist.local_file_exists(local_file_path))
        loaders.gpl01.delete_in_local('a1')
        self.assertFalse(exist.local_file_exists(local_file_path))
