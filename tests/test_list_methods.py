import os
from tests.resources import bucket_name, gs_dir_path, gs_subdir_path, \
    local_dir_path, local_subdir_path
from tests.populate import populate_gs, populate_local
from tests.base_class import BaseClassTest
from tests import loaders


class ListMethodsTest(BaseClassTest):

    def test_list_blobs(self):
        populate_gs()

        self.assertEqual(
            sorted([f'a{i}_gs' for i in range(9, 12)]),
            sorted([b.name for b in loaders.gpl00.list_blobs('a')]))

        self.assertEqual(
            sorted([f'{gs_dir_path}/a{i}_gs' for i in range(10, 11)]),
            sorted([b.name for b in loaders.gpl10.list_blobs('a1')]))

        self.assertEqual(
            sorted([f'{gs_subdir_path}/a{i}_gs' for i in range(7, 13)]),
            sorted([b.name for b in loaders.gpl20.list_blobs('a')]))

        self.assertEqual([], loaders.gpl00.list_blobs('dir'))
        self.assertEqual([], loaders.gpl10.list_blobs('su'))

    def test_list_blob_uris(self):
        populate_gs()

        self.assertEqual(
            sorted([f'gs://{bucket_name}/a{i}_gs' for i in range(9, 12)]),
            sorted(loaders.gpl00.list_blob_uris('a')))

        self.assertEqual(
            sorted([f'gs://{bucket_name}/{gs_dir_path}/a{i}_gs'
                    for i in range(10, 11)]),
            sorted(loaders.gpl10.list_blob_uris('a1')))

        self.assertEqual(
            sorted([f'gs://{bucket_name}/{gs_subdir_path}/a{i}_gs'
                    for i in range(7, 13)]),
            sorted(loaders.gpl20.list_blob_uris('a')))

        self.assertEqual([], loaders.gpl00.list_blob_uris('dir'))
        self.assertEqual([], loaders.gpl10.list_blob_uris('su'))

    def test_list_local_file_paths(self):
        populate_local()

        self.assertEqual(
            sorted([os.path.normpath(
                os.path.join(local_dir_path, f'a{i}_local'))
                for i in range(7, 15)]),
            sorted([os.path.normpath(p) for p
                    in loaders.gpl20.list_local_file_paths('a')]))

        self.assertEqual(
            sorted([os.path.normpath(
                os.path.join(local_subdir_path, f'a{i}_local'))
                for i in range(10, 14)]),
            sorted([os.path.normpath(p) for p
                    in loaders.gpl21.list_local_file_paths('a1')]))

        self.assertEqual([], loaders.gpl21.list_local_file_paths('sub'))
