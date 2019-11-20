from tests.context.loaders import *
from tests.utils import *


class ListMethodsTest(BaseClassTest):

    def test_list_blobs(self):
        populate_bucket()

        self.assertEqual(
            sorted([b.name for b in gpl1.list_blobs('a')]),
            sorted(['a{}_gs'.format(i) for i in range(9, 12)]))

        self.assertEqual(
            sorted([b.name for b in gpl1.list_blobs('a1')]),
            sorted(['a{}_gs'.format(i) for i in range(10, 12)]))

        self.assertEqual(
            sorted([b.name for b in gpl2.list_blobs('a')]),
            sorted(['dir/subdir/a{}_gs'.format(i) for i in range(7, 11)]))

        self.assertEqual(
            sorted([b.name for b in gpl2.list_blobs('a1')]),
            sorted(['dir/subdir/a{}_gs'.format(i) for i in range(10, 11)]))

    def test_list_blob_uris(self):
        populate_bucket()

        self.assertEqual(
            sorted(gpl1.list_blob_uris('a')),
            sorted(['gs://{}'.format(bucket_name) + '/a{}_gs'.format(i)
                    for i in range(9, 12)]))

        self.assertEqual(
            sorted(gpl1.list_blob_uris('a1')),
            sorted(['gs://{}'.format(bucket_name) + '/a{}_gs'.format(i)
                    for i in range(10, 12)]))

        self.assertEqual(
            sorted(gpl2.list_blob_uris('a')),
            sorted(['gs://{}'.format(bucket_name)
                    + '/dir/subdir/a{}_gs'.format(i) for i in range(7, 11)]))

        self.assertEqual(
            sorted(gpl2.list_blob_uris('a1')),
            sorted(['gs://{}'.format(bucket_name)
                    + '/dir/subdir/a{}_gs'.format(i) for i in range(10, 11)]))

    def test_list_local_file_paths(self):
        populate_local_folder()

        self.assertEqual(
            sorted([os.path.normpath(p) for p
                    in gpl3.list_local_file_paths('a')]),
            sorted([os.path.normpath(
                os.path.join(local_dir_path, 'a{}_local'.format(i)))
                for i in range(9, 14)]))

        self.assertEqual(
            sorted([os.path.normpath(p) for p
                    in gpl3.list_local_file_paths('a1')]),
            sorted([os.path.normpath(
                os.path.join(local_dir_path, 'a{}_local'.format(i)))
                for i in range(10, 14)]))
