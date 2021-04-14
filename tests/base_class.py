import os
import shutil
import unittest
from tests.context.resources import bq_client, dataset_ref, bucket, \
    local_dir_path


def empty_dataset():
    tables = bq_client.list_tables(dataset_ref)
    for t in tables:
        bq_client.delete_table(table=t.reference)


def empty_bucket():
    blobs = list(bucket.list_blobs())
    bucket.delete_blobs(blobs=blobs)


def delete_local_folder():
    if os.path.isdir(local_dir_path):
        shutil.rmtree(local_dir_path)


def clean():
    empty_dataset()
    empty_bucket()
    delete_local_folder()


class BaseClassTest(unittest.TestCase):
    def setUp(self):
        clean()
        os.makedirs(local_dir_path)

    def tearDown(self):
        clean()
