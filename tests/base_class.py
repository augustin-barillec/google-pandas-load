import os
import shutil
import unittest
from tests.resources import bq_client, dataset_id, bucket, \
    local_dir_path, local_subdir_path


def empty_dataset():
    tables = bq_client.list_tables(dataset_id)
    for t in tables:
        bq_client.delete_table(table=t.reference)


def empty_bucket():
    blobs = list(bucket.list_blobs())
    bucket.delete_blobs(blobs=blobs)


def create_local_subdir():
    os.makedirs(local_subdir_path)


def delete_local_dir():
    if os.path.isdir(local_dir_path):
        shutil.rmtree(local_dir_path)


def clean():
    empty_dataset()
    empty_bucket()
    delete_local_dir()


class BaseClassTest(unittest.TestCase):
    def setUp(self):
        clean()
        create_local_subdir()

    def tearDown(self):
        clean()
