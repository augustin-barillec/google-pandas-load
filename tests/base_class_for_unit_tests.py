import os
import shutil
import unittest
from tests.context.resources import *


def clean():
    # empty the dataset
    tables = bq_client.list_tables(dataset_ref)
    for t in tables:
        bq_client.delete(table=t.reference)

    # empty the bucket
    blobs = bucket.list_blobs()
    bucket.delete_blobs(blobs=blobs)

    # delete the local folder
    if os.path.isdir(local_dir_path):
        shutil.rmtree(local_dir_path)


class BaseClassTest(unittest.TestCase):
    def setUp(self):
        clean()
        os.makedirs(local_dir_path)

    def tearDown(self):
        clean()

