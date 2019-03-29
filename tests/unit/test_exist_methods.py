import os
import json
from tests.context.loaders import *
from tests.base_class_for_unit_tests import BaseClassTest


class ListMethodsTest(BaseClassTest):

    def test_list_blob_uris_when_gs_dir_path_is_None(self):
        blobs = [storage.Blob(name='a{}'.format(i), bucket=bucket) for i in range(3)]
        for b in blobs:
            b.upload_from_string(data='data')

        expected_res = ['gs://{}'.format(bucket_name) + '/a{}'.format(i) for i in range(3)]

        res = sorted(gpl1.list_blob_uris(data_name='a'))

        self.assertEqual(res, expected_res)

    def test_list_blob_uris_when_gs_dir_path_is_not_None(self):
        blobs = [storage.Blob(name='dir/subdir/a{}'.format(i), bucket=bucket) for i in range(9, 11)]
        for b in blobs:
            b.upload_from_string(data='data')

        expected_res = sorted(['gs://{}'.format(bucket_name) + '/dir/subdir/a{}'.format(i) for i in range(9, 11)])

        res = sorted(gpl2.list_blob_uris(data_name='a'))

        self.assertEqual(res, expected_res)

    def test_list_local_file_paths(self):
        with open(os.path.join(local_dir_path, 'a0'), 'w') as outfile:
            json.dump('data', outfile)

        expected_res = [os.path.join(local_dir_path, 'a0')]

        res = gpl3.list_local_file_paths(data_name='a')

        self.assertEqual(res, expected_res)
