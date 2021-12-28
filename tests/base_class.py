import os
import shutil
import unittest
from tests.resources import bq_client, dataset_id, bucket, \
    local_dir_path, local_subdir_path


def empty_dataset():
    table_names = [t.table_id for t in bq_client.list_tables(dataset_id)]
    for n in table_names:
        table_id = f'{dataset_id}.{n}'
        bq_client.delete_table(table=table_id)


def empty_bucket():
    blobs = list(bucket.list_blobs())
    bucket.delete_blobs(blobs=blobs)


def create_local_subfolder():
    os.makedirs(local_subdir_path)


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
        create_local_subfolder()

    def tearDown(self):
        clean()
