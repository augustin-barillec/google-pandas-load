import os
from tests.utils import constants
from tests.utils.populate import populate_bucket, populate_local
from tests.utils import ids
from tests.utils.loader import create_loader
from tests.utils.base_class import BaseClassTest


class ListTest(BaseClassTest):

    def test_list_blobs(self):
        populate_bucket()

        gpl00 = create_loader()
        gpl10 = create_loader(bucket_dir_path=constants.bucket_dir_path)
        gpl20 = create_loader(bucket_dir_path=constants.bucket_subdir_path)

        self.assertEqual(
            sorted([ids.build_blob_name_0(f'a{i}') for i in range(7, 12)]),
            [b.name for b in gpl00.list_blobs('a')])

        self.assertEqual(
            sorted([ids.build_blob_name_1(f'a{i}') for i in range(10, 13)]),
            [b.name for b in gpl10.list_blobs('a1')])

        self.assertEqual(
            sorted([ids.build_blob_name_2(f'a{i}') for i in range(9, 14)]),
            [b.name for b in gpl20.list_blobs('')])

        self.assertEqual([], gpl00.list_blobs('dir'))
        self.assertEqual([], gpl10.list_blobs('su'))

    def test_list_blob_uris(self):
        populate_bucket()

        gpl00 = create_loader()
        gpl10 = create_loader(bucket_dir_path=constants.bucket_dir_path)
        gpl20 = create_loader(bucket_dir_path=constants.bucket_subdir_path)

        blob_names = [ids.build_blob_name_0(f'a{i}') for i in range(7, 12)]
        blob_uris = [ids.build_bucket_uri(n) for n in blob_names]
        blob_uris = sorted(blob_uris)
        self.assertEqual(blob_uris, gpl00.list_blob_uris('a'))

        blob_names = [ids.build_blob_name_1(f'a{i}') for i in range(10, 13)]
        blob_uris = [ids.build_bucket_uri(n) for n in blob_names]
        blob_uris = sorted(blob_uris)
        self.assertEqual(blob_uris, gpl10.list_blob_uris('a1'))

        blob_names = [ids.build_blob_name_2(f'a{i}') for i in range(9, 14)]
        blob_uris = [ids.build_bucket_uri(n) for n in blob_names]
        blob_uris = sorted(blob_uris)
        self.assertEqual(blob_uris, gpl20.list_blob_uris('a'))

        self.assertEqual([], gpl00.list_blob_uris('dir'))
        self.assertEqual([], gpl10.list_blob_uris('su'))

    def test_list_local_file_paths(self):
        populate_local()
        gpl20 = create_loader(bucket_dir_path=constants.bucket_subdir_path)
        gpl21 = create_loader(
            bucket_dir_path=constants.bucket_subdir_path,
            local_dir_path=constants.local_subdir_path)

        local_file_paths = [ids.build_local_file_path_0(f'a{i}')
                            for i in range(7, 12)]
        local_file_paths = sorted(local_file_paths)
        expected = [os.path.normpath(p) for p in local_file_paths]
        computed = [os.path.normpath(p)
                    for p in gpl20.list_local_file_paths('a')]
        self.assertEqual(expected, computed)

        local_file_paths = [ids.build_local_file_path_1(f'a{i}')
                            for i in range(10, 13)]
        local_file_paths = sorted(local_file_paths)
        expected = [os.path.normpath(p) for p in local_file_paths]
        computed = [os.path.normpath(p)
                    for p in gpl21.list_local_file_paths('a1')]
        self.assertEqual(expected, computed)

        self.assertEqual([], gpl21.list_local_file_paths('sub'))
