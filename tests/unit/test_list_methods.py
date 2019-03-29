import os
import json
from tests.context.loaders import *
from tests.base_class_for_unit_tests import BaseClassTest


class ListMethodsTest(BaseClassTest):

    def test_list_blobs(self):
        blobs_at_root = [storage.Blob(name='a{}'.format(i), bucket=bucket) for i in range(9, 12)]
        blobs_in_dir = [storage.Blob(name='dir/subdir/a{}'.format(i), bucket=bucket) for i in range(9, 14)]

        for b in blobs_at_root + blobs_in_dir:
            b.upload_from_string(data='data')

        self.assertEqual(
            sorted([b.name for b in gpl1.list_blobs(data_name='a')]),
            sorted(['a{}'.format(i) for i in range(9, 12)]))

        self.assertEqual(
            sorted([b.name for b in gpl1.list_blobs(data_name='a1')]),
            sorted(['a{}'.format(i) for i in range(10, 12)]))

        self.assertEqual(
            sorted([b.name for b in gpl2.list_blobs(data_name='a')]),
            sorted(['dir/subdir/a{}'.format(i) for i in range(9, 14)]))

        self.assertEqual(
            sorted([b.name for b in gpl2.list_blobs(data_name='a1')]),
            sorted(['dir/subdir/a{}'.format(i) for i in range(10, 14)]))

    def test_list_blob_uris(self):
        blobs_at_root = [storage.Blob(name='a{}'.format(i), bucket=bucket) for i in range(9, 12)]
        blobs_in_dir = [storage.Blob(name='dir/subdir/a{}'.format(i), bucket=bucket) for i in range(9, 14)]

        for b in blobs_at_root + blobs_in_dir:
            b.upload_from_string(data='data')

        self.assertEqual(
            sorted(gpl1.list_blob_uris(data_name='a')),
            sorted(['gs://{}'.format(bucket_name) + '/a{}'.format(i) for i in range(9, 12)]))

        self.assertEqual(
            sorted(gpl1.list_blob_uris(data_name='a1')),
            sorted(['gs://{}'.format(bucket_name) + '/a{}'.format(i) for i in range(10, 12)]))

        self.assertEqual(
            sorted(gpl2.list_blob_uris(data_name='a')),
            sorted(['gs://{}'.format(bucket_name) + '/dir/subdir/a{}'.format(i) for i in range(9, 14)]))

        self.assertEqual(
            sorted(gpl2.list_blob_uris(data_name='a1')),
            sorted(['gs://{}'.format(bucket_name) + '/dir/subdir/a{}'.format(i) for i in range(10, 14)]))

    def test_list_local_file_paths(self):
        for i in range(9, 12):
            with open(os.path.join(local_dir_path, 'a{}'.format(i)), 'w') as outfile:
                json.dump('data', outfile)

        self.assertEqual(
            sorted(gpl3.list_local_file_paths(data_name='a')),
            sorted([os.path.join(local_dir_path, 'a{}'.format(i)) for i in range(9, 12)]))

        self.assertEqual(
            sorted(gpl3.list_local_file_paths(data_name='a1')),
            sorted([os.path.join(local_dir_path, 'a{}'.format(i)) for i in range(10, 12)]))
