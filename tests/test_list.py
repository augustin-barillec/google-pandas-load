import os
from tests.utils.populate import populate_bucket, populate_local
from tests.utils import ids
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


class ListTest(BaseClassTest):

    def test_list_blobs(self):
        populate_bucket()

        self.assertEqual(
            sorted([ids.build_blob_name_0(f'a{i}') for i in range(7, 12)]),
            [b.name for b in loaders.gpl00.list_blobs('a')])

        self.assertEqual(
            sorted([ids.build_blob_name_1(f'a{i}') for i in range(10, 13)]),
            [b.name for b in loaders.gpl10.list_blobs('a1')])

        self.assertEqual(
            sorted([ids.build_blob_name_2(f'a{i}') for i in range(9, 14)]),
            [b.name for b in loaders.gpl20.list_blobs('')])

        self.assertEqual([], loaders.gpl00.list_blobs('dir'))
        self.assertEqual([], loaders.gpl10.list_blobs('su'))

    def test_list_blob_uris(self):
        populate_bucket()

        blob_names = [ids.build_blob_name_0(f'a{i}') for i in range(7, 12)]
        blob_uris = [ids.build_bucket_uri(n) for n in blob_names]
        blob_uris = sorted(blob_uris)
        self.assertEqual(blob_uris, loaders.gpl00.list_blob_uris('a'))

        blob_names = [ids.build_blob_name_1(f'a{i}') for i in range(10, 13)]
        blob_uris = [ids.build_bucket_uri(n) for n in blob_names]
        blob_uris = sorted(blob_uris)
        self.assertEqual(blob_uris, loaders.gpl10.list_blob_uris('a1'))

        blob_names = [ids.build_blob_name_2(f'a{i}') for i in range(9, 14)]
        blob_uris = [ids.build_bucket_uri(n) for n in blob_names]
        blob_uris = sorted(blob_uris)
        self.assertEqual(blob_uris, loaders.gpl20.list_blob_uris('a'))

        self.assertEqual([], loaders.gpl00.list_blob_uris('dir'))
        self.assertEqual([], loaders.gpl10.list_blob_uris('su'))

    def test_list_local_file_paths(self):
        populate_local()

        local_file_paths = [ids.build_local_file_path_0(f'a{i}')
                            for i in range(7, 12)]
        local_file_paths = sorted(local_file_paths)
        expected = [os.path.normpath(p) for p in local_file_paths]
        computed = [os.path.normpath(p)
                    for p in loaders.gpl20.list_local_file_paths('a')]
        self.assertEqual(expected, computed)

        local_file_paths = [ids.build_local_file_path_1(f'a{i}')
                            for i in range(10, 13)]
        local_file_paths = sorted(local_file_paths)
        expected = [os.path.normpath(p) for p in local_file_paths]
        computed = [os.path.normpath(p)
                    for p in loaders.gpl21.list_local_file_paths('a1')]
        self.assertEqual(expected, computed)

        self.assertEqual([], loaders.gpl21.list_local_file_paths('sub'))
